"""Business logic for the "acquisition_handler" DAG."""

from airflow.models import TaskInstance
from common.keys import DagContext, DagParams, Dags, OpArgs, XComKeys
from common.utils import get_xcom, trigger_dag_run
from file_handling import copy_file
from raw_file_wrapper_factory import RawFileWrapperFactory

from shared.db.interface import get_raw_file_by_id, update_raw_file
from shared.db.models import RawFileStatus


def copy_raw_file(ti: TaskInstance, **kwargs) -> None:
    """Copy a raw file to the target location."""
    del ti  # unused
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]

    raw_file = get_raw_file_by_id(raw_file_id)

    update_raw_file(raw_file_id, new_status=RawFileStatus.COPYING)

    raw_file_copy_wrapper = RawFileWrapperFactory.create_copy_wrapper(
        instrument_id=instrument_id, raw_file=raw_file
    )

    for src_path, dst_path in raw_file_copy_wrapper.get_files_to_copy().items():
        copy_file(src_path, dst_path)

    # TODO: add also hash to DB
    # HERE: try-catch
    file_size = 1  # get_file_size(raw_file_copy_wrapper.file_path_to_calculate_size())
    update_raw_file(raw_file_id, new_status=RawFileStatus.COPYING_DONE, size=file_size)


def start_file_mover(ti: TaskInstance, **kwargs) -> None:
    """Trigger the file_mover DAG for a specific raw file."""
    del ti  # unused
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]

    trigger_dag_run(
        Dags.FILE_MOVER,
        {
            DagParams.RAW_FILE_ID: raw_file_id,
        },
    )


def decide_processing(ti: TaskInstance, **kwargs) -> bool:
    """Decide whether to start the acquisition_processor DAG."""
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]

    acquisition_monitor_errors = get_xcom(ti, XComKeys.ACQUISITION_MONITOR_ERRORS)

    if not acquisition_monitor_errors:
        return True  # continue with downstream tasks

    # potential other checks:
    #  - has 'blank' or 'DDA' in file name -> Variable?
    #  - file size to small -> Variable?

    update_raw_file(raw_file_id, new_status=RawFileStatus.ACQUISITION_FAILED)

    return False  # skip downstream tasks


def start_acquisition_processor(ti: TaskInstance, **kwargs) -> None:
    """Trigger an acquisition_processor DAG run for specific raw files.

    Each raw file is added to the database first.
    Then, for each raw file, the project id is determined.
    Only for raw files that carry a project id, the acquisition_handler DAG is triggered.
    """
    del ti  # unused
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]

    dag_id_to_trigger = f"{Dags.ACQUISITON_HANDLER}.{instrument_id}"

    update_raw_file(raw_file_id, new_status=RawFileStatus.QUEUED_FOR_QUANTING)

    trigger_dag_run(
        dag_id_to_trigger,
        {
            DagParams.RAW_FILE_ID: raw_file_id,
        },
    )
