"""DAG to watch acquisition and trigger follow-up DAGS on demand."""

from __future__ import annotations

from datetime import timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

with DAG(
    "acquisition_watcher.test6",
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
        task_id="wait_for_finished_acquisition",
        bash_command="sleep 10",
    )

    start_acquisition_handler = TriggerDagRunOperator(
        task_id="start_acquisition_handler",
        trigger_dag_id="acquisition_handler.test6",
        conf={"raw_file_name": "some_file.raw"},
    )

wait_for_finished_acquisition >> start_acquisition_handler
