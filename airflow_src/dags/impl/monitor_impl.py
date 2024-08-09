"""Business logic for the "file_handler" DAG."""

from airflow.models import TaskInstance
from common.keys import DagContext, DagParams, Dags, OpArgs
from common.utils import trigger_dag_run


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
