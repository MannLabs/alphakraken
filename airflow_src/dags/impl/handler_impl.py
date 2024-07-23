"""Business logic for the "acquisition_handler" DAG."""

from airflow.models import TaskInstance
from common.keys import DagContext, DagParams, Dags, OpArgs
from common.utils import trigger_dag_run
from file_handling import copy_file, get_file_size
from raw_data_wrapper import RawDataWrapper

from shared.db.interface import get_raw_file_by_id, update_raw_file
from shared.db.models import RawFileStatus


def copy_raw_file(ti: TaskInstance, **kwargs) -> None:
    """Copy a raw file to the target location."""
    del ti  # unused
    raw_file_name = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]

    raw_file = get_raw_file_by_id(raw_file_name)

    update_raw_file(raw_file_name, new_status=RawFileStatus.COPYING)

    raw_data_wrapper = RawDataWrapper.create(
        instrument_id=instrument_id, raw_file=raw_file
    )

    for src_path, dst_path in raw_data_wrapper.get_files_to_copy().items():
        copy_file(src_path, dst_path)

    # TODO: add also hash to DB
    file_size = get_file_size(raw_data_wrapper.file_path_to_watch())
    update_raw_file(
        raw_file_name, new_status=RawFileStatus.COPYING_DONE, size=file_size
    )


def start_acquisition_processor(ti: TaskInstance, **kwargs) -> None:
    """Trigger an acquisition_processor DAG run for specific raw files.

    Each raw file is added to the database first.
    Then, for each raw file, the project id is determined.
    Only for raw files that carry a project id, the acquisition_handler DAG is triggered.
    """
    del ti  # unused
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]
    raw_file_name = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]

    dag_id_to_trigger = f"{Dags.ACQUISITON_HANDLER}.{instrument_id}"

    update_raw_file(raw_file_name, new_status=RawFileStatus.QUEUED_FOR_QUANTING)

    trigger_dag_run(
        dag_id_to_trigger,
        {
            DagParams.RAW_FILE_ID: raw_file_name,
        },
    )
