"""DAG to watch acquisition and trigger follow-up DAGS on demand."""

from __future__ import annotations

from datetime import timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from plugins.shared.keys import Dags, Tasks

with DAG(
    f"{Dags.ACQUISITON_WATCHER}.test6",
    default_args={
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue',
        "trigger_rule": "always",
    },
    description="Watch acquisition and trigger follow-up DAGS on demand.",
    catchup=False,
    tags=["kraken"],
) as dag:
    dag.doc_md = __doc__

    wait_for_finished_acquisition = BashOperator(
        task_id=Tasks.WAIT_FOR_FINISHED_ACQUISITION,
        bash_command="sleep 10",
    )

    start_acquisition_handler = TriggerDagRunOperator(
        task_id=Tasks.START_ACQUISITION_HANDLER,
        trigger_dag_id=f"{Dags.ACQUISITON_HANDLER}.test6",
        conf={"raw_file_name": "some_file.raw"},
    )

wait_for_finished_acquisition >> start_acquisition_handler
