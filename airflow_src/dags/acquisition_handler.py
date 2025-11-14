"""DAG to watch acquisition, copy raw files and trigger follow-up DAGs."""

from __future__ import annotations

from datetime import timedelta

from airflow.models import Param
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from callbacks import on_failure_callback
from common.constants import (
    AIRFLOW_QUEUE_PREFIX,
    Pools,
)
from common.keys import (
    DAG_DELIMITER,
    DagParams,
    Dags,
    OpArgs,
    Tasks,
)
from common.settings import (
    Concurrency,
    Timings,
    get_instrument_ids,
)
from impl.handler_impl import (
    compute_checksum,
    copy_raw_file,
    decide_processing,
    start_acquisition_processor,
    start_file_mover,
)
from impl.s3_upload import upload_raw_file_to_s3
from sensors.acquisition_monitor import AcquisitionMonitor

from shared.yamlsettings import is_s3_upload_enabled


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
            # this maps the DAG to the worker that is responsible for that queue, cf. docker-compose.yaml
            # and https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/celery_executor.html#queues
            "queue": f"{AIRFLOW_QUEUE_PREFIX}{instrument_id}",
            # this callback is executed when tasks fail
            "on_failure_callback": on_failure_callback,
            # finish tasks before tackling new ones, otherwise on catch up all hashsums would be calculated first before any copying
            # cf. https://airflow.apache.org/docs/apache-airflow/2.10.5/administration-and-deployment/priority-weight.html
            "weight_rule": "upstream",
        },
        description="Watch acquisition, handle raw files and trigger follow-up DAGs on demand.",
        catchup=False,
        tags=["handler", instrument_id],
        params={DagParams.RAW_FILE_ID: Param(type="string", minimum=3)},
    ) as dag:
        dag.doc_md = __doc__

        monitor_acquisition_ = AcquisitionMonitor(
            task_id=Tasks.MONITOR_ACQUISITION,
            instrument_id=instrument_id,
            poke_interval=Timings.ACQUISITION_MONITOR_POKE_INTERVAL_S,
            max_active_tis_per_dag=Concurrency.MAXNO_MONITOR_ACQUISITION_TASKS_PER_DAG,
            execution_timeout=timedelta(minutes=Timings.ACQUISITION_MONITOR_TIMEOUT_M),
        )

        compute_checksum_ = ShortCircuitOperator(
            task_id=Tasks.COMPUTE_CHECKSUM,
            python_callable=compute_checksum,
            max_active_tis_per_dag=Concurrency.MAXNO_COPY_RAW_FILE_TASKS_PER_DAG,
            execution_timeout=timedelta(minutes=Timings.RAW_DATA_COPY_TASK_TIMEOUT_M),
            pool=Pools.FILE_COPY_POOL,  # uses file_copy_pool because hash computation also requires file transfer over network
        )

        copy_raw_file_ = PythonOperator(
            task_id=Tasks.COPY_RAW_FILE,
            python_callable=copy_raw_file,
            op_kwargs={OpArgs.INSTRUMENT_ID: instrument_id},
            max_active_tis_per_dag=Concurrency.MAXNO_COPY_RAW_FILE_TASKS_PER_DAG,
            execution_timeout=timedelta(minutes=Timings.RAW_DATA_COPY_TASK_TIMEOUT_M),
            pool=Pools.FILE_COPY_POOL,
        )

        upload_to_s3_ = PythonOperator(
            task_id=Tasks.UPLOAD_TO_S3,
            python_callable=upload_raw_file_to_s3,
            execution_timeout=timedelta(hours=6),  # Large files need time
            retries=3,
            retry_delay=timedelta(minutes=5),
            pool=Pools.S3_UPLOAD_POOL,
        )

        start_file_mover_ = PythonOperator(
            task_id=Tasks.START_FILE_MOVER,
            python_callable=start_file_mover,
            op_kwargs={OpArgs.INSTRUMENT_ID: instrument_id},
        )

        decide_processing_ = ShortCircuitOperator(
            task_id=Tasks.DECIDE_PROCESSING,
            python_callable=decide_processing,
            op_kwargs={OpArgs.INSTRUMENT_ID: instrument_id},
        )

        start_acquisition_processor_ = PythonOperator(
            task_id=Tasks.START_ACQUISITION_PROCESSOR,
            python_callable=start_acquisition_processor,
            op_kwargs={OpArgs.INSTRUMENT_ID: instrument_id},
        )

    first_part = monitor_acquisition_ >> compute_checksum_ >> copy_raw_file_
    second_part = (
        start_file_mover_ >> decide_processing_ >> start_acquisition_processor_
    )

    if not is_s3_upload_enabled():
        (first_part >> second_part)
    else:
        (first_part >> [upload_to_s3_, start_file_mover_])

        upload_to_s3_  # noqa: B018

        second_part  # noqa: B018


for instrument_id in get_instrument_ids():
    create_acquisition_handler_dag(instrument_id)
