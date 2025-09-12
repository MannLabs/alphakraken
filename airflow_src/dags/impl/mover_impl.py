"""Implementation of tasks for file_mover."""

import logging
import shutil
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance
from common.keys import DagContext, DagParams, XComKeys
from common.utils import get_env_variable, get_xcom, put_xcom
from file_handling import compare_paths, get_file_size
from raw_file_wrapper_factory import (
    MovePathProvider,
    RawFileWrapperFactory,
    get_main_file_size_from_db,
)

from shared.db.interface import get_raw_file_by_id, update_raw_file
from shared.db.models import InstrumentFileStatus, RawFile
from shared.keys import EnvVars


def get_files_to_move(ti: TaskInstance, **kwargs) -> None:
    """Get single files to move for a raw_file_id to the instrument backup folder."""
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]

    raw_file = get_raw_file_by_id(raw_file_id)

    move_wrapper = RawFileWrapperFactory.create_write_wrapper(
        raw_file, path_provider=MovePathProvider
    )

    files_to_move = move_wrapper.get_files_to_move()
    main_file_to_move = move_wrapper.main_file_path()

    put_xcom(
        ti, XComKeys.FILES_TO_MOVE, {str(k): str(v) for k, v in files_to_move.items()}
    )
    put_xcom(ti, XComKeys.MAIN_FILE_TO_MOVE, str(main_file_to_move))


def move_files(ti: TaskInstance, **kwargs) -> None:
    """Move all files/folders associated with a raw file to the instrument backup folder."""
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]
    raw_file = get_raw_file_by_id(raw_file_id)

    files_to_move_str = get_xcom(ti, XComKeys.FILES_TO_MOVE)
    files_to_move = {Path(k): Path(v) for k, v in files_to_move_str.items()}

    files_to_actually_move, files_to_actually_remove = _get_files_to_move(files_to_move)

    if files_to_actually_move or files_to_actually_remove:
        try:
            _check_main_file_to_move(ti, raw_file)
        except FileNotFoundError as e:
            logging.warning(f"File not found: {e}")
        _move_files(files_to_actually_move)
        _move_files(files_to_actually_remove, only_rename=True)

        update_raw_file(raw_file_id, instrument_file_status=InstrumentFileStatus.MOVED)


def _get_files_to_move(
    files_to_check: dict[Path, Path],
) -> tuple[dict[Path, Path], dict[Path, Path]]:
    """Check if the files to move are in a consistent state with the destination.

    - source does not exist, but target does -> skip
    - source does not exist and target does not exist -> raise
    - source exists and target does not exist -> mark for moving
    - source exists and target exists, not equivalent -> raise
    - source exists and target exists, equivalent -> mark for removing
    """
    files_to_move = {}
    files_to_remove = {}
    for src_path, dst_path in files_to_check.items():
        if not src_path.exists():
            if not dst_path.exists():
                msg = f"Neither {src_path=} nor {dst_path=} exist."
                # this is some weird state, better raise for now to enable investigation.
                raise AirflowFailException(msg)

            msg = f"File {src_path=} does not exist, but {dst_path=} does. Presuming it was moved before."
            logging.info(msg)
            continue

        if dst_path.exists():
            missing_files, different_files, items_only_in_target = compare_paths(
                src_path, dst_path
            )
            logging.info(f"File {dst_path=} already exists:")
            logging.info(f"  Files missing in target: {missing_files}")
            logging.info(f"  Files different in target: {different_files}")
            logging.info(f"  Files only in target: {items_only_in_target}")

            if missing_files or different_files:
                raise AirflowFailException(f"{dst_path=} exists.")

            files_to_remove[src_path] = dst_path
            continue

        files_to_move[src_path] = dst_path

    return files_to_move, files_to_remove


def _move_files(files_to_move: dict[Path, Path], *, only_rename: bool = False) -> None:
    """Move files.

    :param files_to_move: dictionary mapping source to destination paths
    :param only_rename: if True, only rename the source file (add a .deleteme extension to it)
    """
    env_name = get_env_variable(EnvVars.ENV_NAME)

    for src_path, dst_path in files_to_move.items():
        # security measure to not have sandbox interfere with production
        if env_name != "production":
            logging.warning(
                f"NOT moving raw file {src_path} to {dst_path}: not in production."
            )
            continue

        if not dst_path.parent.exists():
            logging.info(f"Creating directory {dst_path.parent}")
            dst_path.parent.mkdir(parents=True, exist_ok=True)

        if not only_rename:
            logging.info(f"Moving raw file {src_path} to {dst_path}")

            shutil.move(src_path, dst_path)
            logging.info(".. done")
        # except OSError as e:
        #     # sometimes for Thermo raw files the `unlink` operation that is done in the course of `move`
        #     # fails due to "Device or resource busy".
        #     # In this case, try to rename the source file to facilitate manual cleanup.
        #     if not src_path.is_file():
        #         raise e from e
        #     force_rename = True

        else:
            # the new name _MUST_ have a different extension (than .d), otherwise
            # it will be picked up as a new file
            new_name = f"{src_path!s}.deleteme"
            logging.info(f"Renaming {src_path} to {new_name} ..")
            src_path.rename(new_name)
            logging.info(".. done")


def _check_main_file_to_move(ti: TaskInstance, raw_file: RawFile) -> None:
    """Check if the file size matches the database record.

    This is a safety measure to avoid moving the wrong files in the following (hypothetical) scenario:
    The DAG run to move a certain `raw_file_id` runs with a huge delay, during which the raw file on the
    instrument data path was replaced with a new one with the same name. Because the move operation needs
    to use the `raw_file.original_name`, this could lead to the wrong file being moved.

    Given the very low probability of this happening, a check on the file size of the main file should suffice.

    :param ti: TaskInstance object
    :param raw_file: RawFile from DB

    :raises: AirflowFailException if the file size does not match the database record.
    """
    main_file_to_move = Path(get_xcom(ti, XComKeys.MAIN_FILE_TO_MOVE))

    if (size_current := get_file_size(main_file_to_move)) != (
        size_db := get_main_file_size_from_db(raw_file)
    ):
        raise AirflowFailException(
            f"File size mismatch for {main_file_to_move}: {size_current=} {size_db=}. "
        )
