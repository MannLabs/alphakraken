"""Implementation of tasks for cluster_job_poller."""

from __future__ import annotations

import logging
from time import monotonic, sleep
from typing import TYPE_CHECKING

from airflow.sdk.exceptions import AirflowException
from common.constants import CLUSTER_SSH_COMMAND_TIMEOUT, SLURM_QUEUE_STATES_CMD
from common.keys import AirflowVars
from common.settings import Timings
from common.utils import get_airflow_variable
from paramiko.ssh_exception import SSHException
from sensors.ssh_utils import get_cluster_ssh_hook

from shared.db.interface import update_slurm_jobs

if TYPE_CHECKING:
    from airflow.providers.ssh.hooks.ssh import SSHHook


def poll_slurm_job_states(ssh_connection_id_prefix: str) -> None:
    """Write the states of the queued Slurm jobs to the DB, repeatedly for `CLUSTER_POLLER_LOOP_DURATION_M`.

    One SSH connection is reused for all queries, a new one is only opened after an error.
    """
    if (
        get_airflow_variable(AirflowVars.DEBUG_NO_CLUSTER_SSH, "false").lower()
        == "true"
    ):
        logging.warning(
            f"Variable {AirflowVars.DEBUG_NO_CLUSTER_SSH} set: not polling cluster."
        )
        return

    end = monotonic() + Timings.CLUSTER_POLLER_LOOP_DURATION_M * 60
    attempt_no = 0
    ssh_hook: SSHHook | None = None
    try:
        while monotonic() < end:
            try:
                if ssh_hook is None:
                    ssh_hook = get_cluster_ssh_hook(
                        ssh_connection_id_prefix, attempt_no
                    )
                exit_status, stdout, stderr = ssh_hook.exec_ssh_client_command(
                    ssh_hook.get_conn(),  # cached by the hook while its connection is alive
                    SLURM_QUEUE_STATES_CMD,
                    get_pty=False,
                    environment={},
                    timeout=CLUSTER_SSH_COMMAND_TIMEOUT,
                )
            except (SSHException, AirflowException, OSError) as e:
                logging.warning(f"Exception while polling cluster: {e}")
                if ssh_hook is not None and ssh_hook.client is not None:
                    ssh_hook.client.close()
                ssh_hook = None
                attempt_no += 1
                sleep(Timings.CLUSTER_POLLER_RECONNECT_DELAY_S)
                continue

            if exit_status == 0:
                update_slurm_jobs(
                    ssh_connection_id_prefix,
                    _parse_squeue_output(stdout.decode("utf-8")),
                )
            else:
                logging.warning(f"Polling cluster failed: {exit_status=} {stderr=}")

            sleep(Timings.CLUSTER_POLLER_INTERVAL_S)
    finally:
        if ssh_hook is not None and ssh_hook.client is not None:
            ssh_hook.client.close()


def _parse_squeue_output(output: str) -> dict[str, str]:
    """Parse lines "<job id> <state>" to a dict job id -> state."""
    return dict(line.split() for line in output.splitlines() if line)
