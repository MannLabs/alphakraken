"""Business logic for the "file_handler" DAG."""

import hashlib
import logging
import shutil
from datetime import datetime
from pathlib import Path

from airflow.models import TaskInstance
from common.keys import DagContext, DagParams, Dags, OpArgs
from common.settings import (
    get_internal_instrument_backup_path,
    get_internal_instrument_data_path,
)
from common.utils import trigger_dag_run
from impl.watcher_impl import _get_file_size

from shared.db.interface import update_raw_file
from shared.db.models import RawFileStatus


def update_raw_file_status(ti: TaskInstance, **kwargs) -> None:
    """Update the status of the raw file in the database."""
    del ti  # unused
    raw_file_name = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_NAME]

    update_raw_file(raw_file_name, new_status=RawFileStatus.ACQUISITION_STARTED)


def _get_file_hash(file_path: Path, chunk_size: int = 8192) -> str:
    """Get the hash of a file."""
    with open(file_path, "rb") as f:  # noqa: PTH123
        file_hash = hashlib.md5()  # noqa: S324
        while chunk := f.read(chunk_size):
            file_hash.update(chunk)
    logging.info(f"Hash of {file_path} is {file_hash.hexdigest()}")
    return file_hash.hexdigest()


def copy_raw_file(ti: TaskInstance, **kwargs) -> None:
    """Copy a raw file to the target location."""
    del ti  # unused
    raw_file_name = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_NAME]
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]

    update_raw_file(raw_file_name, new_status=RawFileStatus.COPYING)

    # TODO: this needs to be vendor-specific
    _copy_raw_file(raw_file_name, instrument_id)

    file_size = _get_file_size(raw_file_name, instrument_id)
    update_raw_file(
        raw_file_name, new_status=RawFileStatus.COPYING_FINISHED, size=file_size
    )


def _file_already_exists(dst_path: Path, src_hash: str) -> bool:
    if dst_path.exists():
        logging.info("File already exists in backup location. Checking hash ..")
        if _get_file_hash(dst_path) == (src_hash):
            logging.info("Hashes match.")
            return True
        logging.warning("Hashes do not match.")
    return False


def _copy_raw_file(
    raw_file_name: str,
    instrument_id: str,
) -> None:
    """Copy a raw file to the backup location and check its hashsum."""
    src_path = get_internal_instrument_data_path(instrument_id) / raw_file_name
    dst_path = get_internal_instrument_backup_path(instrument_id) / raw_file_name

    logging.info(f"Copying {src_path} to {dst_path} ..")
    start = datetime.now()  # noqa: DTZ005

    src_hash = _get_file_hash(src_path)
    if _file_already_exists(dst_path, src_hash):
        return

    shutil.copy2(src_path, dst_path)
    time_elapsed = (datetime.now() - start).total_seconds()  # noqa: DTZ005
    dst_size = dst_path.stat().st_size
    logging.info(
        f"Copying done! {dst_size / max(time_elapsed, 1) / 1024 ** 2:.1f} MB/s"
    )

    if (hash_dst := _get_file_hash(dst_path)) != (src_hash):
        raise ValueError(f"Hashes do not match ofter copy! {src_hash=} != {hash_dst=}")


def start_acquisition_handler(ti: TaskInstance, **kwargs) -> None:
    """Trigger an acquisition_handler DAG run for specific raw files.

    Each raw file is added to the database first.
    Then, for each raw file, the project id is determined.
    Only for raw files that carry a project id, the file_handler DAG is triggered.
    """
    del ti  # unused
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]
    raw_file_name = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_NAME]

    dag_id_to_trigger = f"{Dags.ACQUISITON_HANDLER}.{instrument_id}"

    trigger_dag_run(
        dag_id_to_trigger,
        {
            DagParams.RAW_FILE_NAME: raw_file_name,
        },
    )
