"""Implementation of tasks for file_mover."""

import logging
import shutil

from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance
from common.keys import DagContext, DagParams, XComKeys
from common.settings import (
    INSTRUMENT_BACKUP_FOLDER_NAME,
    get_internal_instrument_data_path,
)
from common.utils import get_env_variable, get_xcom, put_xcom
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

    put_xcom(ti, XComKeys.FILES_TO_MOVE, files_to_move)


def move_files(ti: TaskInstance, **kwargs) -> None:
    """Move files/folders to the instrument backup folder."""
    del kwargs  # unused

    files_to_move = get_xcom(ti, XComKeys.FILES_TO_MOVE)

    for src_path, dst_path in files_to_move.items():
        if not src_path.exists():
            msg = f"File {src_path=} does not exist, but {dst_path=} does."
            if not dst_path.exists():
                msg = f"Neither {src_path=} nor {dst_path=} exist."
            raise AirflowFailException(msg)

        if dst_path.exists():
            # missing_files, different_files, items_only_in_target = compare_directories(src_path, dst_path)
            # logging.warning(f"Files missing in target: {missing_files}")
            # logging.warning(f"Files different in target: {different_files}")
            # logging.warning(f"Files only in target: {items_only_in_target}")

            raise AirflowFailException(f"File {dst_path=} already exists.")

    for src_path, dst_path in files_to_move.items():
        # security measure to not have sandbox interfere with production
        if get_env_variable(EnvVars.ENV_NAME) != "production":
            logging.warning(
                f"NOT moving raw file {src_path} to {dst_path}: not in production."
            )
            continue

        dst_path.parent.mkdir(parents=True, exist_ok=True)

        logging.info(f"Moving raw file {src_path} to {dst_path}")
        shutil.move(src_path, dst_path)
