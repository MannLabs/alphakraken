"""DAG to continuously write the states of the jobs on the Slurm clusters to the DB."""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from common.constants import AIRFLOW_QUEUE_PREFIX
from common.keys import Dags, Tasks
from common.settings import Timings
from impl.poller_impl import poll_slurm_job_states

from shared.keys import JobEngines
from shared.runners import RUNNERS


def create_cluster_job_poller_dag(ssh_connection_id_prefixes: list[str]) -> None:
    """Create cluster_job_poller dag with one polling task per cluster."""
    with DAG(
        Dags.CLUSTER_JOB_POLLER,
        # a new run starts as soon as the previous one finished
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
            "queue": f"{AIRFLOW_QUEUE_PREFIX}cluster_job_poller",
        },
        description="Write the states of the jobs on the Slurm clusters to the DB.",
        tags={"cluster_job_poller"},
    ) as dag:
        dag.doc_md = __doc__

        for ssh_connection_id_prefix in ssh_connection_id_prefixes:
            PythonOperator(
                task_id=f"{Tasks.POLL_SLURM_JOB_STATES}_{ssh_connection_id_prefix}",
                python_callable=poll_slurm_job_states,
                op_kwargs={"ssh_connection_id_prefix": ssh_connection_id_prefix},
                execution_timeout=timedelta(
                    minutes=2 * Timings.CLUSTER_POLLER_LOOP_DURATION_M
                ),
            )


# runners sharing a cluster share its connections, so poll per connection prefix
_slurm_ssh_connection_id_prefixes = sorted(
    {
        runner.ssh_connection_id_prefix
        for runner in RUNNERS.values()
        if runner.engine == JobEngines.SLURM and runner.ssh_connection_id_prefix
    }
)

# a continuous DAG without tasks would start new runs in a tight loop
if _slurm_ssh_connection_id_prefixes:
    create_cluster_job_poller_dag(_slurm_ssh_connection_id_prefixes)
