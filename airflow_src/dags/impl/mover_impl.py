"""Implementation of tasks for file_mover."""

import logging
import shutil
from pathlib import Path
from typing import Any

from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance
from common.keys import DagContext, DagParams, XComKeys
from common.settings import (
    INSTRUMENT_BACKUP_FOLDER_NAME,
    get_internal_instrument_data_path,
)
from common.utils import get_env_variable, get_xcom, put_xcom
from file_handling import compare_paths, get_file_size
from raw_file_wrapper_factory import RawFileWrapperFactory

from shared.db.interface import get_raw_file_by_id
from shared.keys import EnvVars


def get_files_to_move(ti: TaskInstance, **kwargs) -> None:
    """Get files to move to the instrument backup folder."""
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]

    raw_file = get_raw_file_by_id(raw_file_id)
    instrument_id = raw_file.instrument_id

    target_path = (
        get_internal_instrument_data_path(instrument_id) / INSTRUMENT_BACKUP_FOLDER_NAME
    )
    move_wrapper = RawFileWrapperFactory.create_copy_wrapper(
        instrument_id, raw_file, target_path
    )

    files_to_move = move_wrapper.get_files_to_move()
    file_path_to_calculate_size = move_wrapper.file_path_to_calculate_size()

    put_xcom(
        ti, XComKeys.FILES_TO_MOVE, {str(k): str(v) for k, v in files_to_move.items()}
    )
    put_xcom(ti, XComKeys.MAIN_FILE_TO_MOVE, str(file_path_to_calculate_size))


def move_files(ti: TaskInstance, **kwargs) -> None:
    """Move files/folders to the instrument backup folder if their size matches the database record."""
    _check_main_file_to_move(ti, kwargs)

    files_to_move_str = get_xcom(ti, XComKeys.FILES_TO_MOVE)
    files_to_move = {Path(k): Path(v) for k, v in files_to_move_str.items()}

    for src_path, dst_path in files_to_move.items():
        if not src_path.exists():
            if not dst_path.exists():
                msg = f"Neither {src_path=} nor {dst_path=} exist."
            else:
                # in the future, this might not be an error, but a warning
                msg = f"File {src_path=} does not exist, but {dst_path=} does."
            raise AirflowFailException(msg)

        if dst_path.exists():
            missing_files, different_files, items_only_in_target = compare_paths(
                src_path, dst_path
            )
            logging.info(f"Files missing in target: {missing_files}")
            logging.info(f"Files different in target: {different_files}")
            logging.info(f"Files only in target: {items_only_in_target}")

            raise AirflowFailException(f"File {dst_path=} already exists.")

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
