"""DAG to watch acquisition and trigger follow-up DAGS on demand."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from common.keys import (
    DAG_DELIMITER,
    Dags,
    OpArgs,
    Tasks,
)
from common.settings import INSTRUMENTS, Timings
from impl.watcher_impl import get_raw_files, start_acquisition_handler
from sensors.file_sensor import FileCreationSensor


def create_acquisition_watcher_dag(instrument_id: str) -> None:
    """Create acquisition_watcher dag for instrument with `instrument_id`."""
    with DAG(
        f"{Dags.ACQUISITON_WATCHER}{DAG_DELIMITER}{instrument_id}",
        default_args={
            "depends_on_past": False,
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            # 'queue': 'bash_queue',
        },
        description="Watch acquisition and trigger follow-up DAGs on demand.",
        catchup=False,
        tags=["kraken"],
        start_date=pendulum.datetime(2000, 1, 1, tz="UTC"),
        schedule="@continuous",
        max_active_runs=1,
    ) as dag:
        dag.doc_md = __doc__

        wait_for_file_creation_ = FileCreationSensor(
            task_id=Tasks.WAIT_FOR_FILE_CREATION,
            instrument_id=instrument_id,
            poke_interval=Timings.FILE_CREATION_POKE_INTERVAL_S,
        )

        get_raw_files_ = PythonOperator(
            task_id=Tasks.GET_RAW_FILES,
            python_callable=get_raw_files,
            op_kwargs={OpArgs.INSTRUMENT_ID: instrument_id},
        )

        start_acquisition_handler_ = PythonOperator(
            task_id=Tasks.START_ACQUISITION_HANDLER,
            python_callable=start_acquisition_handler,
            op_kwargs={OpArgs.INSTRUMENT_ID: instrument_id},
        )

    wait_for_file_creation_ >> get_raw_files_ >> start_acquisition_handler_


for instrument_id in INSTRUMENTS:
    create_acquisition_watcher_dag(instrument_id)
