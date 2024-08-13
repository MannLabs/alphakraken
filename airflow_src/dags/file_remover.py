"""DAG to remove files from instrument backup folder."""

from __future__ import annotations

from datetime import timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from common.keys import (
    Dags,
    Tasks,
)
from common.settings import (
    AIRFLOW_QUEUE_PREFIX,
    Concurrency,
    Timings,
)
from impl.remover_impl import get_raw_files_to_remove, remove_raw_files


def create_file_remover_dag() -> None:
    """Create file_remover dag."""
    with DAG(
        f"{Dags.FILE_REMOVER}",
        schedule=None,
        # these are the default arguments for each TASK
        default_args={
            "depends_on_past": False,
            "schedule_interval": "0 7 * * *",  # run every morning
            "retries": 4,
            "retry_delay": timedelta(minutes=5),
            # this maps the DAG to the worker that is responsible for that queue, cf. docker-compose.yaml
            # and https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/celery_executor.html#queues
            "queue": f"{AIRFLOW_QUEUE_PREFIX}file_mover",
        },
        description="Remove files from backup folder on instrument.",
        catchup=False,
        tags=["file_remover"],
    ) as dag:
        dag.doc_md = __doc__

        get_files_to_remove_ = PythonOperator(
            task_id=Tasks.GET_RAW_FILES_TO_REMOVE,
            python_callable=get_raw_files_to_remove,
        )

        remove_raw_files_ = PythonOperator(
            task_id=Tasks.REMOVE_RAW_FILES,
            python_callable=remove_raw_files,
            max_active_tis_per_dag=Concurrency.MAXNO_MOVE_RAW_FILE_TASKS_PER_DAG,
            execution_timeout=timedelta(minutes=Timings.MOVE_RAW_FILE_TASK_TIMEOUT_M),
        )

    get_files_to_remove_ >> remove_raw_files_


create_file_remover_dag()
