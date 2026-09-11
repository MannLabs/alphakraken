"""Utility functions for SSH operations."""

import base64
import json
import logging
from time import sleep

from airflow.exceptions import AirflowException, AirflowFailException
from common.constants import EXIT_CODE_FILE_NAME, LAUNCHER_SCRIPT_STEM
from common.keys import AirflowVars, JobStates
from common.utils import get_airflow_variable, get_cluster_ssh_hook, truncate_string
from paramiko.ssh_exception import SSHException

# both the simple_ssh and the pueue engine hide their windows script behind this powershell flag
_ENCODED_COMMAND_FLAG = "-EncodedCommand "


def ssh_execute(
    command: str,
    ssh_connection_id_prefix: str,
    *,
    max_tries: int = 30,
) -> str:
    """Execute the given `command` over one of the SSH connections with the given prefix.

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

    call_count = 0
    str_stdout = None
    while True:
        if call_count >= max_tries:
            logging.error(f"Execution of SSH command failed too often: {command=}")
            raise AirflowFailException("Execution of SSH command failed too often.")

        sleep(60 * call_count)  # no sleep in the first iteration
        call_count += 1

        try:
            ssh_hook = get_cluster_ssh_hook(
                attempt_no=call_count - 1,
                ssh_connection_id_prefix=ssh_connection_id_prefix,
            )
            exit_status, agg_stdout, agg_stderr = ssh_hook.exec_ssh_client_command(
                ssh_hook.get_conn(),
                command,
                timeout=60,
                get_pty=False,
                environment={},
            )
        except (
            SSHException,  # "Timeout opening channel."
            AirflowException,  # "SSH command timed out"
        ) as e:
            logging.warning(f"Exception while executing SSH command: {e}")
            continue

        str_stdout = _byte_to_string(agg_stdout)
        str_stdout_trunc = truncate_string(str_stdout)

        logging.info(
            f"ssh command call #{call_count} returned: {exit_status=} {str_stdout_trunc=} {agg_stderr=}"
        )

        # in rare cases, exit_status=0 but agg_stderr contains an error message
        error_messages = ["Batch job submission failed"]
        if (str_stderr := _byte_to_string(agg_stderr)) and any(
            e in str_stderr for e in error_messages
        ):
            logging.warning(f"SSH command returned error: {str_stderr}")
            continue

        if exit_status == 0 and agg_stdout not in [b"", b"\n"]:
            break

    assert str_stdout is not None  # for type checker

    return str_stdout


def _byte_to_string(input_: bytes | None) -> str | None:
    """Convert the given `input_` to a string, return None if None is passed."""
    if input_ is None:
        return None

    return input_.decode("utf-8").strip()


def _get_fake_ssh_response(command: str) -> str:
    """Fake an ssh response for the given `command`.

    Only for testing and debugging.
    """
    logging.warning(
        f"Variable {AirflowVars.DEBUG_NO_CLUSTER_SSH} set: Not running SSH command on cluster:\n{command}"
    )
    # very heuristic way to decide which fake response to return
    script = _decode_powershell_command(command)
    if "sbatch" in script:  # run job
        response = "something\nsomething\n123"
    elif "TIME_ELAPSED" in script:  # get job info
        response = f"00:00:01\nsomething\n{JobStates.COMPLETED}"
    elif LAUNCHER_SCRIPT_STEM in script:  # simple_ssh: run job
        response = "123"
    elif EXIT_CODE_FILE_NAME in script:  # simple_ssh: get job info
        response = f"{JobStates.COMPLETED} 1"
    elif "pueue add" in script:  # pueue: run job
        response = "123"
    elif "pueue status" in script:  # pueue: get job info
        response = json.dumps(
            {
                "tasks": {
                    "123": {
                        "status": {
                            "Done": {
                                "start": "1970-01-01T00:00:00+00:00",
                                "end": "1970-01-01T00:00:01+00:00",
                                "result": "Success",
                            }
                        }
                    }
                }
            }
        )
    else:
        response = JobStates.COMPLETED  # monitor job

    logging.warning(f"Returning fake response: {response}")
    return response


def _decode_powershell_command(command: str) -> str:
    """Get the script a windows command carries, so the heuristics above match on its content.

    Without this, every `-EncodedCommand` looks alike: the payload is base64 of UTF-16LE.
    """
    if _ENCODED_COMMAND_FLAG not in command:
        return command

    encoded = command.split(_ENCODED_COMMAND_FLAG)[-1]
    return base64.b64decode(encoded).decode("utf-16-le")
