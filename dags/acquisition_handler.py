"""DAG to handle acquisition."""

from __future__ import annotations

import sys
from datetime import timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

# TODO: find a better way, this is required to unify module import between docker and bash
sys.path.insert(0, "/opt/airflow/")
from plugins.handler_plugins import (
    compute_metrics,
    monitor_quanting,
    prepare_quanting,
    run_quanting,
    upload_metrics,
)
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

        prepare_quanting_ = PythonOperator(
            task_id=Tasks.PREPARE_QUANTING, python_callable=prepare_quanting
        )

        run_quanting_ = PythonOperator(
            task_id=Tasks.RUN_QUANTING, python_callable=run_quanting
        )

        monitor_quanting_ = PythonOperator(
            task_id=Tasks.MONITOR_QUANTING, python_callable=monitor_quanting
        )

        compute_metrics_ = PythonOperator(
            task_id=Tasks.COMPUTE_METRICS, python_callable=compute_metrics
        )

        upload_metrics_ = PythonOperator(
            task_id=Tasks.UPLOAD_METRICS, python_callable=upload_metrics
        )

    (
        prepare_quanting_
        >> run_quanting_
        >> monitor_quanting_
        >> compute_metrics_
        >> upload_metrics_
    )


for instrument_id in INSTRUMENTS:
    create_acquisition_handler_dag(instrument_id)
