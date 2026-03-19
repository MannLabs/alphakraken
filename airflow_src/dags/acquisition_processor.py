"""DAG to process acquisition data: quant and extract metrics."""

from __future__ import annotations

from datetime import timedelta

from airflow.decorators import task, task_group
from airflow.models import Param, TaskInstance
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.utils.trigger_rule import TriggerRule
from callbacks import on_failure_callback
from common.constants import AIRFLOW_QUEUE_PREFIX, Pools
from common.keys import (
    DAG_DELIMITER,
    DagParams,
    Dags,
    QuantingEnv,
    TaskGroups,
    Tasks,
    XComKeys,
)
from common.settings import (
    Concurrency,
    Timings,
    get_instrument_ids_with_value,
)
from common.utils import get_minutes_since_fixed_time_point, put_xcom
from impl.processor_impl import (
    check_quanting_result,
    compute_metrics,
    finalize_raw_file_status,
    prepare_quanting,
    run_quanting,
    upload_metrics,
)
from sensors.ssh_sensor import (
    WaitForJobFinishSensor,
    WaitForJobStartSensor,
)

from shared.yamlsettings import YamlKeys


def create_acquisition_processor_dag(instrument_id: str) -> None:
    """Create acquisition_processor dag for instrument with `instrument_id`."""
    with DAG(
        f"{Dags.ACQUISITION_PROCESSOR}{DAG_DELIMITER}{instrument_id}",
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
            # make sure that downstream tasks are executed before any upstream tasks
            # to make sure the cluster_slots_pool works correctly ("run_quanting" should only run if all "monitoring" tasks are done)
            # cf. https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/priority-weight.html
            "weight_rule": "upstream",
        },
        description="Process acquired files and add metrics to DB.",
        catchup=False,
        tags=["processor", instrument_id],
        params={DagParams.RAW_FILE_ID: Param(type="string", minimum=3)},
    ) as dag:
        dag.doc_md = __doc__

        @task(
            task_id=Tasks.PREPARE_QUANTING,
            # make the youngest created task the one with the highest prio (last in, first out)
            priority_weight=get_minutes_since_fixed_time_point(),
        )
        def prepare_quanting_task(params: dict | None = None) -> list[dict]:
            assert params is not None
            return prepare_quanting(
                raw_file_id=params[DagParams.RAW_FILE_ID],
                instrument_id=instrument_id,
            )

        @task_group(group_id=TaskGroups.QUANTING_PIPELINE)
        def quanting_pipeline(quanting_env: dict) -> None:
            @task
            def push_quanting_env(
                quanting_env: dict, ti: TaskInstance | None = None
            ) -> None:
                """Bridge task: push the mapped quanting_env parameter to XCom for downstream classic operators."""
                put_xcom(ti, XComKeys.QUANTING_ENV, quanting_env)
                put_xcom(
                    ti, XComKeys.RAW_FILE_ID, quanting_env[QuantingEnv.RAW_FILE_ID]
                )

            push_ = push_quanting_env(quanting_env)

            run_ = PythonOperator(
                task_id=Tasks.RUN_QUANTING,
                python_callable=run_quanting,
                pool=Pools.CLUSTER_SLOTS_POOL,
            )

            wait_ = WaitForJobStartSensor(
                task_id=Tasks.WAIT_FOR_JOB_START,
                poke_interval=Timings.JOB_MONITOR_POKE_INTERVAL_S,
                max_active_tis_per_dag=Concurrency.MAXNO_JOB_MONITOR_TASKS_PER_DAG,
                pool=Pools.CLUSTER_SLOTS_POOL,
            )

            monitor_ = WaitForJobFinishSensor(
                task_id=Tasks.MONITOR_QUANTING,
                poke_interval=Timings.JOB_MONITOR_POKE_INTERVAL_S,
                max_active_tis_per_dag=Concurrency.MAXNO_JOB_MONITOR_TASKS_PER_DAG,
                # Note: if we decouple this task from cluster_slots_pool, then this setting would steer only the
                #  number of 'pending' jobs, which might be more desirable in some cases.
                pool=Pools.CLUSTER_SLOTS_POOL,
            )

            check_ = ShortCircuitOperator(
                task_id=Tasks.CHECK_QUANTING_RESULT,
                python_callable=check_quanting_result,
            )

            compute_ = PythonOperator(
                task_id=Tasks.COMPUTE_METRICS,
                python_callable=compute_metrics,
            )

            upload_ = PythonOperator(
                task_id=Tasks.UPLOAD_METRICS,
                python_callable=upload_metrics,
            )

            push_ >> run_ >> wait_ >> monitor_ >> check_ >> compute_ >> upload_

        @task(
            task_id=Tasks.FINALIZE_STATUS,
            trigger_rule=TriggerRule.ALL_DONE,
        )
        def finalize_status(
            params: dict | None = None, ti: TaskInstance | None = None
        ) -> None:
            assert params is not None
            finalize_raw_file_status(
                ti=ti,
                raw_file_id=params[DagParams.RAW_FILE_ID],
            )

        envs = prepare_quanting_task()
        mapped = quanting_pipeline.expand(quanting_env=envs)
        mapped >> finalize_status()


for instrument_id, _ in get_instrument_ids_with_value(YamlKeys.TYPE):
    create_acquisition_processor_dag(instrument_id)
