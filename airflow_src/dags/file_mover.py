"""DAG to move files to backup folder on instrument disk."""

from __future__ import annotations

from datetime import timedelta

from airflow.models import Param
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from common.keys import (
    DagParams,
    Dags,
    Tasks,
)
from common.settings import (
    AIRFLOW_QUEUE_PREFIX,
)
from impl.mover_impl import move_raw_file


def create_file_mover_dag() -> None:
    """Create file_mover dag."""
    with DAG(
        f"{Dags.FILE_MOVER}",
        schedule=None,
        # these are the default arguments for each TASK
        default_args={
            "depends_on_past": False,
            "retries": 4,
            "retry_delay": timedelta(minutes=1),
            # this maps the DAG to the worker that is responsible for that queue, cf. docker-compose.yaml
            # and https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/celery_executor.html#queues
            "queue": f"{AIRFLOW_QUEUE_PREFIX}file_mover",
        },
        description="Move file from acquisition folder to backup folder on instrument.",
        catchup=False,
        tags=["file_mover"],
        params={DagParams.RAW_FILE_ID: Param(type="string", minimum=3)},
    ) as dag:
        dag.doc_md = __doc__

        move_raw_file_ = PythonOperator(
            task_id=Tasks.MOVE_RAW_FILE,
            python_callable=move_raw_file,
            # max_active_tis_per_dag=Concurrency.MAXNO_COPY_RAW_FILE_TASKS_PER_DAG,
            # execution_timeout=timedelta(minutes=Timings.RAW_DATA_COPY_TASK_TIMEOUT_M),
        )

    move_raw_file_  # noqa: B018


create_file_mover_dag()
