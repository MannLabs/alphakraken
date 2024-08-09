"""DAG to handle acquisition."""

from __future__ import annotations

import sys
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

# TODO: find a better way, this is required to find the shared module in docker-compose
sys.path.insert(0, "/opt/airflow/")
from shared.keys import DAG_DELIMITER, Dags, Tasks
from shared.settings import INSTRUMENTS


def create_acquisition_handler_dag(instrument_id: str) -> None:
    """Create acquisition_handler dag for instrument with `instrument_id`."""
    with DAG(
        f"{Dags.ACQUISITON_HANDLER}{DAG_DELIMITER}{instrument_id}",
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


for instrument_id in INSTRUMENTS:
    create_acquisition_handler_dag(instrument_id)
