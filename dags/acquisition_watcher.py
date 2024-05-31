"""DAG to watch acquisition and trigger follow-up DAGS on demand."""

from __future__ import annotations

import sys
from datetime import timedelta

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# TODO: find a better way, this is required to unify module import between docker and bash
sys.path.insert(0, "/opt/airflow/")
from plugins.watcher_plugins import wait_for_finished_acquisition
from shared.keys import (
    DAG_DELIMITER,
    DagParams,
    Dags,
    OpArgs,
    Tasks,
)
from shared.settings import INSTRUMENTS


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

        wait_for_finished_acquisition_ = PythonOperator(
            task_id=Tasks.WAIT_FOR_FINISHED_ACQUISITION,
            python_callable=wait_for_finished_acquisition,
            op_kwargs={OpArgs.INSTRUMENT_ID: instrument_id},
        )

        start_acquisition_handler = TriggerDagRunOperator(
            task_id=Tasks.START_ACQUISITION_HANDLER,
            trigger_dag_id=f"{Dags.ACQUISITON_HANDLER}.{instrument_id}",
            # example how to pass parameters to the python callable
            conf={DagParams.RAW_FILE_NAME: "some_file.raw"},
        )

    wait_for_finished_acquisition_ >> start_acquisition_handler


for instrument_id in INSTRUMENTS:
    create_acquisition_watcher_dag(instrument_id)
