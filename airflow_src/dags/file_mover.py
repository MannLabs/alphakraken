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
    Concurrency,
    Timings,
)
from impl.mover_impl import get_files_to_move, move_files


def create_file_mover_dag() -> None:
    """Create file_mover dag."""
    with DAG(
        f"{Dags.FILE_MOVER}",
        schedule=None,
        catchup=False,
        # these are the default arguments for each TASK
        default_args={
            "depends_on_past": False,
            "retries": 4,
            "retry_delay": timedelta(minutes=5),
            # this maps the DAG to the worker that is responsible for that queue, cf. docker-compose.yaml
            # and https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/celery_executor.html#queues
            "queue": f"{AIRFLOW_QUEUE_PREFIX}file_mover",
        },
        description="Move file from acquisition folder to backup folder on instrument.",
        tags=["file_mover"],
        params={DagParams.RAW_FILE_ID: Param(type="string", minimum=3)},
    ) as dag:
        dag.doc_md = __doc__

        get_files_to_move_ = PythonOperator(
            task_id=Tasks.GET_FILES_TO_MOVE,
            python_callable=get_files_to_move,
        )

        move_raw_files_ = PythonOperator(
            task_id=Tasks.MOVE_RAW_FILES,
            python_callable=move_files,
            max_active_tis_per_dag=Concurrency.MAXNO_MOVE_RAW_FILE_TASKS_PER_DAG,
            execution_timeout=timedelta(minutes=Timings.MOVE_RAW_FILE_TASK_TIMEOUT_M),
        )

    get_files_to_move_ >> move_raw_files_


create_file_mover_dag()
