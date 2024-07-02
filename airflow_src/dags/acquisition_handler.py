"""DAG to watch acquisition and handle raw files."""

from __future__ import annotations

from datetime import timedelta

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from callbacks import on_failure_callback
from common.keys import (
    DAG_DELIMITER,
    Dags,
    OpArgs,
    Tasks,
)
from common.settings import (
    AIRFLOW_QUEUE_PREFIX,
    INSTRUMENTS,
    Concurrency,
    Pools,
    Timings,
)
from impl.handler_impl import (
    copy_raw_file,
    start_acquisition_processor,
    update_raw_file_status,
)
from sensors.acquisition_monitor import AcquisitionMonitor


def create_acquisition_handler_dag(instrument_id: str) -> None:
    """Create acquisition_handler dag for instrument with `instrument_id`."""
    with DAG(
        f"{Dags.ACQUISITION_HANDLER}{DAG_DELIMITER}{instrument_id}",
        schedule=None,
        # these are the default arguments for each TASK
        default_args={
            "depends_on_past": False,
            "retries": 4,
            "retry_delay": timedelta(minutes=1),
            # this maps the DAG to the worker that is responsible for that queue, cf. docker-compose.yml
            # and https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/celery_executor.html#queues
            "queue": f"{AIRFLOW_QUEUE_PREFIX}{instrument_id}",
            # this callback is executed when tasks fail
            "on_failure_callback": on_failure_callback,
        },
        description="Watch acquisition, handle raw files and trigger follow-up DAGs on demand.",
        catchup=False,
        tags=["acquisition_handler", instrument_id],
    ) as dag:
        dag.doc_md = __doc__

        update_raw_file_ = PythonOperator(
            task_id=Tasks.UPDATE_RAW_FILE_STATUS,
            python_callable=update_raw_file_status,
            op_kwargs={OpArgs.INSTRUMENT_ID: instrument_id},
        )

        monitor_acquisition_ = AcquisitionMonitor(
            task_id=Tasks.MONITOR_ACQUISITION,
            instrument_id=instrument_id,
            poke_interval=Timings.ACQUISITION_MONITOR_POKE_INTERVAL_S,
        )

        copy_raw_file_ = PythonOperator(
            task_id=Tasks.COPY_RAW_FILE,
            python_callable=copy_raw_file,
            op_kwargs={OpArgs.INSTRUMENT_ID: instrument_id},
            # limit the number of concurrent copies to not over-stress the network.
            # Note that this is a potential bottleneck, so a timeout is important here.
            max_active_tis_per_dag=Concurrency.MAX_ACTIVE_COPY_TASKS_PER_DAG,
            execution_timeout=timedelta(Timings.FILE_COPY_TIMEOUT_M),
            pool=Pools.FILE_COPY_POOL,
        )

        start_acquisition_processor_ = PythonOperator(
            task_id=Tasks.START_ACQUISITION_PROCESSOR,
            python_callable=start_acquisition_processor,
            op_kwargs={OpArgs.INSTRUMENT_ID: instrument_id},
        )

    (
        update_raw_file_
        >> monitor_acquisition_
        >> copy_raw_file_
        >> start_acquisition_processor_
    )


for instrument_id in INSTRUMENTS:
    create_acquisition_handler_dag(instrument_id)
