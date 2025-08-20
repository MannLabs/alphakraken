"""DAG to process acquisition data: quant and extract metrics."""

from __future__ import annotations

from datetime import timedelta
from functools import partial

from airflow.models import Param
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from callbacks import on_failure_callback
from common.constants import AIRFLOW_QUEUE_PREFIX, Pools
from common.keys import DAG_DELIMITER, DagParams, Dags, OpArgs, Tasks, XComKeys
from common.settings import (
    Concurrency,
    Timings,
    get_instrument_ids_with_value,
)
from common.utils import get_minutes_since_fixed_time_point
from impl.processor_impl import (
    check_quanting_result,
    compute_metrics,
    prepare_quanting,
    run_quanting,
    upload_metrics,
)
from sensors.ssh_sensor import (
    WaitForJobFinishSensor,
    WaitForJobStartSensor,
)

from shared.db.models import RawFileStatus
from shared.keys import InstrumentTypes, MetricsTypes
from shared.yamlsettings import YamlKeys

ACTIVATE_MSQC = False  # TODO: temporary flag


def create_acquisition_processor_dag(instrument_id: str, instrument_type: str) -> None:
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

        prepare_quanting_ = PythonOperator(
            task_id=Tasks.PREPARE_QUANTING,
            python_callable=prepare_quanting,
            op_kwargs={OpArgs.INSTRUMENT_ID: instrument_id},
            # make the youngest created task the one with the highest prio (last in, first out)
            priority_weight=get_minutes_since_fixed_time_point(),
        )

        do_msqc = (
            ACTIVATE_MSQC and instrument_type != InstrumentTypes.SCIEX
        )  # TODO: hack to prevent msqc running for sciex. Remedy: vendor-specific fallbacks
        if do_msqc:
            run_msqc_ = PythonOperator(
                task_id=Tasks.RUN_MSQC,
                python_callable=partial(
                    run_quanting,
                    job_script_name="submit_msqc_job.sh",  # TODO: use command instead
                    xcom_key_job_id=XComKeys.MSQC_JOB_ID,
                    new_status=RawFileStatus.COMPUTING_MSQC,
                ),
                pool=Pools.CLUSTER_SLOTS_POOL,
            )
            monitor_msqc_ = WaitForJobFinishSensor(
                task_id=Tasks.MONITOR_MSQC,
                poke_interval=10,
                max_active_tis_per_dag=Concurrency.MAXNO_JOB_MONITOR_TASKS_PER_DAG,
                # Note: if we decouple this task from cluster_slots_pool, then this setting would steer only the
                #  number of 'pending' jobs, which might be more desirable in some cases.
                pool=Pools.CLUSTER_SLOTS_POOL,
                xcom_key_job_id=XComKeys.MSQC_JOB_ID,
            )
            compute_msqc_metrics_ = PythonOperator(
                task_id=Tasks.COMPUTE_MSQC_METRICS,
                python_callable=partial(
                    compute_metrics,
                    metrics_type=MetricsTypes.MSQC,
                    add_quanting_time_elapsed=False,
                ),
            )
            upload_msqc_metrics_ = PythonOperator(
                task_id=Tasks.UPLOAD_MSQC_METRICS,
                python_callable=partial(upload_metrics, metrics_type=MetricsTypes.MSQC),
            )

        run_quanting_ = PythonOperator(
            task_id=Tasks.RUN_QUANTING,
            python_callable=partial(run_quanting, output_path_check=not do_msqc),
            pool=Pools.CLUSTER_SLOTS_POOL,
        )

        wait_for_job_start_ = WaitForJobStartSensor(
            task_id=Tasks.WAIT_FOR_JOB_START,
            poke_interval=Timings.JOB_MONITOR_POKE_INTERVAL_S,
            max_active_tis_per_dag=Concurrency.MAXNO_JOB_MONITOR_TASKS_PER_DAG,
            pool=Pools.CLUSTER_SLOTS_POOL,
        )

        monitor_quanting_ = WaitForJobFinishSensor(
            task_id=Tasks.MONITOR_QUANTING,
            poke_interval=Timings.JOB_MONITOR_POKE_INTERVAL_S,
            max_active_tis_per_dag=Concurrency.MAXNO_JOB_MONITOR_TASKS_PER_DAG,
            # Note: if we decouple this task from cluster_slots_pool, then this setting would steer only the
            #  number of 'pending' jobs, which might be more desirable in some cases.
            pool=Pools.CLUSTER_SLOTS_POOL,
        )

        check_quanting_result_ = ShortCircuitOperator(
            task_id=Tasks.CHECK_QUANTING_RESULT,
            python_callable=check_quanting_result,
        )

        compute_metrics_ = PythonOperator(
            task_id=Tasks.COMPUTE_METRICS, python_callable=compute_metrics
        )

        upload_metrics_ = PythonOperator(
            task_id=Tasks.UPLOAD_METRICS, python_callable=upload_metrics
        )

    if ACTIVATE_MSQC:
        (prepare_quanting_ >> [run_msqc_, run_quanting_])

        (run_msqc_ >> monitor_msqc_ >> compute_msqc_metrics_ >> upload_msqc_metrics_)

        (
            run_quanting_
            >> wait_for_job_start_
            >> monitor_quanting_
            >> check_quanting_result_
            >> compute_metrics_
            >> upload_metrics_
        )

    else:
        (
            prepare_quanting_
            >> run_quanting_
            >> wait_for_job_start_
            >> monitor_quanting_
            >> check_quanting_result_
            >> compute_metrics_
            >> upload_metrics_
        )


for instrument_id, instrument_type in get_instrument_ids_with_value(YamlKeys.TYPE):
    create_acquisition_processor_dag(instrument_id, instrument_type)
