"""DAG to watch acquisition and trigger follow-up DAGS on demand."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from callbacks import on_failure_callback
from common.constants import AIRFLOW_QUEUE_PREFIX, Timings
from common.keys import (
    DAG_DELIMITER,
    Dags,
    OpArgs,
    Tasks,
)
from common.yaml_wrapper import get_instrument_ids
from impl.watcher_impl import (
    decide_raw_file_handling,
    get_unknown_raw_files,
    start_acquisition_handler,
)
from sensors.file_sensor import FileCreationSensor


def create_instrument_watcher_dag(instrument_id: str) -> None:
    """Create instrument_watcher dag for instrument with `instrument_id`."""
    with DAG(
        f"{Dags.ACQUISITION_WATCHER}{DAG_DELIMITER}{instrument_id}",
        schedule="@continuous",
        start_date=pendulum.datetime(2000, 1, 1, tz="UTC"),
        max_active_runs=1,
        catchup=False,
        # these are the default arguments for each TASK
        default_args={
            "depends_on_past": False,
            "retries": 4,
            "retry_delay": timedelta(minutes=1),
            # this maps the DAG to the worker that is responsible for that queue, cf. docker-compose.yaml
            # and https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/celery_executor.html#queues
            "queue": f"{AIRFLOW_QUEUE_PREFIX}{instrument_id}",
            # this callback is executed when tasks fail
            "on_failure_callback": on_failure_callback,
            # Make sure the watcher tasks always have highest priority among all tasks in the system.
            # As some weights use "epoch", set this to a very large value (should be fine until the year 2128)
            "priority_weight": 20000000,
        },
        description="Watch for new files.",
        tags=[
            "watcher",
            instrument_id,
        ],
    ) as dag:
        dag.doc_md = __doc__

        wait_for_raw_file_creation_ = FileCreationSensor(
            task_id=Tasks.WAIT_FOR_RAW_FILE_CREATION,
            instrument_id=instrument_id,
            poke_interval=Timings.FILE_SENSOR_POKE_INTERVAL_S,
        )

        get_unknown_raw_files_ = PythonOperator(
            task_id=Tasks.GET_UNKNOWN_RAW_FILES,
            python_callable=get_unknown_raw_files,
            op_kwargs={OpArgs.INSTRUMENT_ID: instrument_id},
        )

        decide_raw_file_handling_ = PythonOperator(
            task_id=Tasks.DECIDE_HANDLING,
            python_callable=decide_raw_file_handling,
            op_kwargs={OpArgs.INSTRUMENT_ID: instrument_id},
        )

        start_acquisition_handler_ = PythonOperator(
            task_id=Tasks.START_ACQUISITION_HANDLER,
            python_callable=start_acquisition_handler,
            op_kwargs={OpArgs.INSTRUMENT_ID: instrument_id},
            # No retries: on error, a new DAG run should take care of the remaining files.
            # as otherwise we could end up in an inconsistent state (some files already in DB, some not)
            # Also, collisions would be treated twice.
            retries=0,
        )

    (
        wait_for_raw_file_creation_
        >> get_unknown_raw_files_
        >> decide_raw_file_handling_
        >> start_acquisition_handler_
    )


for instrument_id in get_instrument_ids():
    create_instrument_watcher_dag(instrument_id)
