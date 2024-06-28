"""Business logic for the "file_handler" DAG."""

import logging
from datetime import datetime

import pytz
from airflow.api.common.trigger_dag import trigger_dag
from airflow.models import DagRun, TaskInstance
from airflow.utils.types import DagRunType
from common.keys import DagContext, DagParams, Dags, OpArgs


def start_acquisition_handler(ti: TaskInstance, **kwargs) -> None:
    """Trigger an acquisition_handler DAG run for specific raw files.

    Each raw file is added to the database first.
    Then, for each raw file, the project id is determined.
    Only for raw files that carry a project id, the file_handler DAG is triggered.
    """
    del ti  # unused
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]
    raw_file_name = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_NAME]

    logging.info(f"Got {raw_file_name=}")

    dag_id_to_trigger = f"{Dags.ACQUISITON_HANDLER}.{instrument_id}"

    run_id = DagRun.generate_run_id(
        DagRunType.MANUAL, execution_date=datetime.now(tz=pytz.utc)
    )
    logging.info(
        f"Triggering DAG {dag_id_to_trigger} with {run_id=} for {raw_file_name=}."
    )
    trigger_dag(
        dag_id=dag_id_to_trigger,
        run_id=run_id,
        conf={
            DagParams.RAW_FILE_NAME: raw_file_name,
        },
        replace_microseconds=False,
    )
