"""DAG to handle acquisition."""

from __future__ import annotations

from datetime import timedelta

from airflow.models import Param
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from callbacks import on_failure_callback
from common.keys import DAG_DELIMITER, Dags, OpArgs, Tasks
from common.settings import AIRFLOW_QUEUE_PREFIX, INSTRUMENTS, Concurrency, Timings
from impl.processor_impl import (
    compute_metrics,
    get_job_info,
    prepare_quanting,
    run_quanting,
    upload_metrics,
)
from sensors.ssh_sensor import QuantingSSHSensor

ssh_hook = SSHHook(ssh_conn_id="cluster-conn", conn_timeout=60, cmd_timeout=60)


def create_acquisition_processor_dag(instrument_id: str) -> None:
    """Create acquisition_processor dag for instrument with `instrument_id`."""
    with DAG(
        f"{Dags.ACQUISITON_HANDLER}{DAG_DELIMITER}{instrument_id}",
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
        description="Handle acquisition.",
        catchup=False,
        tags=["acquisition_processor", instrument_id],
        params={"raw_file_name": Param(type="string", minimum=3)},
    ) as dag:
        dag.doc_md = __doc__

        prepare_quanting_ = PythonOperator(
            task_id=Tasks.PREPARE_QUANTING,
            python_callable=prepare_quanting,
            op_kwargs={OpArgs.INSTRUMENT_ID: instrument_id},
        )

        run_quanting_ = PythonOperator(
            task_id=Tasks.RUN_QUANTING,
            python_callable=run_quanting,
            op_kwargs={OpArgs.SSH_HOOK: ssh_hook},
        )

        monitor_quanting_ = QuantingSSHSensor(
            task_id=Tasks.MONITOR_QUANTING,
            ssh_hook=ssh_hook,
            poke_interval=Timings.QUANTING_MONITOR_POKE_INTERVAL_S,
            max_active_tis_per_dag=Concurrency.MAX_ACTIVE_QUANTING_MONITORINGS_PER_DAG,
        )

        get_job_info_ = PythonOperator(
            task_id=Tasks.GET_JOB_INFO,
            python_callable=get_job_info,
            op_kwargs={OpArgs.SSH_HOOK: ssh_hook},
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
        >> get_job_info_
        >> compute_metrics_
        >> upload_metrics_
    )


for instrument_id in INSTRUMENTS:
    create_acquisition_processor_dag(instrument_id)
