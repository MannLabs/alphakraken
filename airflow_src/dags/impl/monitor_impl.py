"""Business logic for the "file_handler" DAG."""

import logging
import shutil
from pathlib import Path

from airflow.models import TaskInstance
from common.keys import DagContext, DagParams, Dags, OpArgs
from common.settings import InternalPaths
from common.utils import trigger_dag_run
from impl.watcher_impl import _get_file_size

from shared.db.interface import update_raw_file
from shared.db.models import RawFileStatus


def update_raw_file_status(ti: TaskInstance, **kwargs) -> None:
    """Update the status of the raw file in the database."""
    del ti  # unused
    raw_file_name = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_NAME]

    update_raw_file(raw_file_name, new_status=RawFileStatus.ACQUISITION_STARTED)


def copy_raw_file(ti: TaskInstance, **kwargs) -> None:
    """Copy a raw file to the target location."""
    del ti  # unused
    raw_file_name = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_NAME]
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]

    update_raw_file(raw_file_name, new_status=RawFileStatus.COPYING)

    src = InternalPaths.MOUNTS_PATH / "instruments" / instrument_id / raw_file_name
    dst = InternalPaths.MOUNTS_PATH / "backup" / instrument_id / raw_file_name

    logging.info(f"Preparing copying {src} to {dst} ..")

    if instrument_id == "test2":
        logging.info(f"Copying {src} to {dst} ..")

        shutil.copy2(src, dst)
        logging.info("Copying done!")

        size_src = Path(src).stat().st_size
        size_dst = Path(dst).stat().st_size
        assert size_src == size_dst, f"Size mismatch: {size_src} != {size_dst}"

    file_size = _get_file_size(raw_file_name, instrument_id)

    update_raw_file(
        raw_file_name, new_status=RawFileStatus.COPYING_FINISHED, size=file_size
    )


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
