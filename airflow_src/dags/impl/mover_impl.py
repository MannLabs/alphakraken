"""Implementation of tasks for file_mover."""

import logging
import shutil

from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance
from common.keys import DagContext, DagParams
from common.settings import (
    INSTRUMENT_BACKUP_FOLDER_NAME,
    get_internal_instrument_data_path,
)
from common.utils import get_env_variable
from raw_file_wrapper_factory import RawFileWrapperFactory

from shared.db.interface import get_raw_file_by_id
from shared.keys import EnvVars


def move_raw_file(ti: TaskInstance, **kwargs) -> None:
    """Move a raw file to the instrument backup folder."""
    del ti  # unused
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

    # temporary deactivate for bruker until solution for moving dirs is found
    # if INSTRUMENTS[instrument_id]["type"] == "bruker":
    #     raise AirflowFailException()

    for src_path, dst_path in files_to_move.items():
        if not src_path.exists():
            if not dst_path.exists():
                raise AirflowFailException(
                    f"Neither {src_path=} nor {dst_path=} exist."
                )
            raise AirflowFailException(
                f"File {src_path=} does not exist, but {dst_path=} does."
            )

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
