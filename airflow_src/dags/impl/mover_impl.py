"""Implementation of tasks for file_mover."""

import logging
import shutil

from airflow.models import TaskInstance
from common.keys import DagContext, DagParams
from common.settings import (
    INSTRUMENT_BACKUP_FOLDER_NAME,
    get_internal_instrument_data_path,
)
from common.utils import get_env_variable

from shared.db.interface import get_raw_file_by_id
from shared.keys import EnvVars


def move_raw_file(ti: TaskInstance, **kwargs) -> None:
    """Move a raw file to the instrument backup folder."""
    del ti  # unused
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]

    raw_file = get_raw_file_by_id(raw_file_id)
    instrument_id = raw_file.instrument_id

    src_path = get_internal_instrument_data_path(instrument_id) / raw_file.original_name
    dst_path = (
        get_internal_instrument_data_path(instrument_id)
        / INSTRUMENT_BACKUP_FOLDER_NAME
        / raw_file.original_name
    )

    if not src_path.exists():
        raise FileNotFoundError(f"File {src_path} does not exist.")

    if dst_path.exists():
        raise FileExistsError(f"File {dst_path} already exists.")

    # security measure to not have sandbox interfere with production
    if get_env_variable(EnvVars.ENV_NAME) != "production":
        logging.warning(
            f"NOT moving raw file {src_path} to {dst_path}: not in production."
        )
        return

    dst_path.parent.mkdir(parents=True, exist_ok=True)

    logging.info(f"Moving raw file {src_path} to {dst_path}")
    shutil.move(src_path, dst_path)
