"""Utility functions for SSH operations."""

import logging
from time import sleep

from airflow.exceptions import AirflowFailException, AirflowException
from airflow.providers.ssh.hooks.ssh import SSHHook
from common.keys import AirflowVars, JobStates
from common.utils import get_airflow_variable, get_cluster_ssh_hook, truncate_string
from paramiko.ssh_exception import SSHException


def ssh_execute(
    command: str,
    *,
    max_tries: int = 30,
) -> str:
    """Execute the given `command`.

    Sometimes the SSH command returns a nonzero exit status '254' or empty byte string,
    in this case it is retried until it is 200 and nonempty until `max_tries` is reached.
    """
    # This is a hack to prevent jobs to be run on the cluster, useful for debugging and initial setup.
    # To get rid of this, e.g. set up a container with a fake ssh server
    if (
        get_airflow_variable(AirflowVars.DEBUG_NO_CLUSTER_SSH, "false").lower()
        == "true"
    ):
        return _get_fake_ssh_response(command)

    str_stdout = ""
    exit_status = None
    agg_stdout = None
    call_count = 0
    while exit_status != 0 or agg_stdout in [b"", b"\n"]:
        if call_count >= max_tries:
            logging.error(f"Execution of SSH command failed too often: {command=}")
            raise AirflowFailException("Execution of SSH command failed too often.")

        sleep(60 * call_count)  # no sleep in the first iteration
        call_count += 1

        try:
            ssh_hook = get_cluster_ssh_hook(attempt_no=call_count-1)
            exit_status, agg_stdout, agg_stderr = ssh_hook.exec_ssh_client_command(
                ssh_hook.get_conn(),
                command,
                timeout=60,
                get_pty=False,
                environment={},
            )
        except (SSHException, # "Timeout opening channel."
                AirflowException # "SSH command timed out"
                ) as e:
            logging.warning(f"Exception while executing SSH command: {e}")
            continue

        str_stdout = _byte_to_string(agg_stdout)
        str_stdout_trunc = truncate_string(str_stdout)

        logging.info(
            f"ssh command call #{call_count} returned: {exit_status=} {str_stdout_trunc=} {agg_stderr=}"
        )

    return str_stdout


def _byte_to_string(input_: bytes) -> str:
    """Convert the given `input_` to a string."""
    return input_.decode("utf-8").strip()


def _get_fake_ssh_response(command: str) -> str:
    """Fake an ssh response for the given `command`.

    Only for testing and debugging.
    """
    logging.warning(
        f"Variable {AirflowVars.DEBUG_NO_CLUSTER_SSH} set: Not running SSH command on cluster:\n{command}"
    )
    # very heuristic way to decide which fake response to return
    if "sbatch" in command:  # run job
        response = "something\nsomething\n123"
    elif "TIME_ELAPSED" in command:  # get job info
        response = f"00:00:01\nsomething\n{JobStates.COMPLETED}"
    else:
        response = JobStates.COMPLETED  # monitor job

    logging.warning(f"Returning fake response: {response}")
    return response
