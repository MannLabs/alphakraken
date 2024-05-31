"""DAG to handle acquisition."""

from __future__ import annotations

from datetime import timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

from plugins.shared.keys import Dags, Tasks

with DAG(
    f"{Dags.ACQUISITON_HANDLER}.test6",
    default_args={
        "depends_on_past": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
        # 'queue': 'bash_queue'
    },
    description="Handle acquisition.",
    catchup=False,
    tags=["kraken"],
) as dag:
    dag.doc_md = __doc__

    prepare_quanting = BashOperator(
        task_id=Tasks.PREPARE_QUANTING,
        bash_command="sleep 10",
    )
    run_quanting = BashOperator(
        task_id=Tasks.RUN_QUANTING,
        bash_command="sleep 10",
    )
    monitor_quanting = BashOperator(
        task_id=Tasks.MONITOR_QUANTING,
        bash_command="sleep 10",
    )
    compute_metrics = BashOperator(
        task_id=Tasks.COMPUTE_METRICS,
        bash_command="sleep 10",
    )
    upload_metrics = BashOperator(
        task_id=Tasks.UPLOAD_METRICS,
        bash_command="sleep 10",
    )

(
    prepare_quanting
    >> run_quanting
    >> monitor_quanting
    >> compute_metrics
    >> upload_metrics
)
