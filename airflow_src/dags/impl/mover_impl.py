"""Implementation of tasks for file_mover."""

import logging
import shutil
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance
from common.keys import DagContext, DagParams, XComKeys
from common.utils import get_env_variable, get_xcom, put_xcom
from file_handling import compare_paths, get_file_size
from raw_file_wrapper_factory import MovePathProvider, RawFileWrapperFactory

from shared.db.interface import get_raw_file_by_id
from shared.keys import EnvVars


def get_files_to_move(ti: TaskInstance, **kwargs) -> None:
    """Get single files to move for a raw_file_id to the instrument backup folder."""
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]

    raw_file = get_raw_file_by_id(raw_file_id)

    move_wrapper = RawFileWrapperFactory.create_write_wrapper(
        raw_file, path_provider=MovePathProvider
    )

    files_to_move = move_wrapper.get_files_to_move()
    file_path_to_calculate_size = move_wrapper.file_path_to_calculate_size()

    put_xcom(
        ti, XComKeys.FILES_TO_MOVE, {str(k): str(v) for k, v in files_to_move.items()}
    )
    put_xcom(ti, XComKeys.MAIN_FILE_TO_MOVE, str(file_path_to_calculate_size))


def move_files(ti: TaskInstance, **kwargs) -> None:
    """Move all files/folders associated with a raw file to the instrument backup folder."""
    _check_main_file_to_move(ti, kwargs)

    files_to_move_str = get_xcom(ti, XComKeys.FILES_TO_MOVE)
    files_to_move = {Path(k): Path(v) for k, v in files_to_move_str.items()}

    files_to_actually_move = _get_files_to_move(files_to_move)

    _move_files(files_to_actually_move)


def _get_files_to_move(files_to_check: dict[Path, Path]) -> dict[Path, Path]:
    """Check if the files to move are in a consistent state with the destination.

    - source does not exist, but target does -> OK
    - source does not exist and target does not exist -> raise
    - source exists and target does not exist -> OK
    - source exists and target exists:
        - if they are equal -> OK
        - if not -> raise
    """
    files_to_move = {}
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

            if not (missing_files + different_files + items_only_in_target):
                logging.warning(f"{dst_path=} is identical to {src_path=}.")
                continue

            raise AirflowFailException(
                f"{dst_path=} exists and is different to {src_path=}"
            )

        files_to_move[src_path] = dst_path

    return files_to_move


def _move_files(files_to_move: dict[Path, Path]) -> None:
    """Move the files."""
    env_name = get_env_variable(EnvVars.ENV_NAME)

    for src_path, dst_path in files_to_move.items():
        # security measure to not have sandbox interfere with production
        if env_name != "production":
            logging.warning(
                f"NOT moving raw file {src_path} to {dst_path}: not in production."
            )
            continue

        logging.info(f"Creating directory {dst_path.parent}")
        dst_path.parent.mkdir(parents=True, exist_ok=True)

        logging.info(f"Moving raw file {src_path} to {dst_path}")
        shutil.move(src_path, dst_path)


def _check_main_file_to_move(
    ti: TaskInstance, context: dict[str, dict[str, Any]]
) -> None:
    """Check if the file size matches the database record.

    This is a safety measure to avoid moving the wrong files in the following (hypothetical) scenario:
    The DAG run to move a certain `raw_file_id` runs with a huge delay, during which the raw file on the
    instrument data path was replaced with a new one with the same name. Because the move operation needs
    to use the `raw_file.original_name`, this could lead to the wrong file being moved.

    Given the very low probability of this happening, a check on the file size of the main file should suffice.

    :param ti: TaskInstance object
    :param context: DAG context

    :raises: AirflowFailException if the file size does not match the database record.
    """
    raw_file_id = context[DagContext.PARAMS][DagParams.RAW_FILE_ID]

    file_path_to_calculate_size_str = get_xcom(ti, XComKeys.MAIN_FILE_TO_MOVE)
    file_path_to_calculate_size = Path(file_path_to_calculate_size_str)
    raw_file = get_raw_file_by_id(raw_file_id)

    if (current_size := get_file_size(file_path_to_calculate_size)) != raw_file.size:
        raise AirflowFailException(
            f"File size mismatch for {file_path_to_calculate_size}. Current: {current_size}, DB: {raw_file.size}. "
        )
