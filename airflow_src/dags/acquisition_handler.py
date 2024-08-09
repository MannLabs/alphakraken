"""DAG to handle acquisition."""

from __future__ import annotations

from datetime import timedelta

from airflow.models import Param
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from common.keys import DAG_DELIMITER, Dags, OpArgs, Tasks
from common.settings import AIRFLOW_QUEUE_PREFIX, INSTRUMENTS, Timings
from impl.handler_impl import (
    add_to_db,
    compute_metrics,
    prepare_quanting,
    run_quanting,
    upload_metrics,
)
from sensors.ssh_sensor import QuantingSSHSensor

ssh_hook = SSHHook(ssh_conn_id="cluster-conn", conn_timeout=60, cmd_timeout=60)


def create_acquisition_handler_dag(instrument_id: str) -> None:
    """Create acquisition_handler dag for instrument with `instrument_id`."""
    with DAG(
        f"{Dags.ACQUISITON_HANDLER}{DAG_DELIMITER}{instrument_id}",
        schedule=None,
        default_args={
            "depends_on_past": False,
            "retries": 5,
            "retry_delay": timedelta(minutes=5),
            # this maps the DAG to the worker that is responsible for that queue, cf. docker-compose.yml
            # and https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/celery_executor.html#queues
            "queue": f"{AIRFLOW_QUEUE_PREFIX}{instrument_id}",
        },
        description="Handle acquisition.",
        catchup=False,
        tags=["kraken"],
        params={"raw_file_name": Param(type="string", minimum=3)},
    ) as dag:
        dag.doc_md = __doc__

        add_to_db_ = PythonOperator(
            task_id=Tasks.ADD_TO_DB,
            python_callable=add_to_db,
            op_kwargs={OpArgs.INSTRUMENT_ID: instrument_id},
        )

        prepare_quanting_ = PythonOperator(
            task_id=Tasks.PREPARE_QUANTING, python_callable=prepare_quanting
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
        )
        # TODO: task config: max runtime
        #  error handling!

        compute_metrics_ = PythonOperator(
            task_id=Tasks.COMPUTE_METRICS, python_callable=compute_metrics
        )

        upload_metrics_ = PythonOperator(
            task_id=Tasks.UPLOAD_METRICS, python_callable=upload_metrics
        )

    (
        add_to_db_
        >> prepare_quanting_
        >> run_quanting_
        >> monitor_quanting_
        >> compute_metrics_
        >> upload_metrics_
    )


for instrument_id in INSTRUMENTS:
    create_acquisition_handler_dag(instrument_id)
