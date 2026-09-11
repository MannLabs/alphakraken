"""Implementation of a job handler that queues jobs with pueue on a machine reachable via SSH.

`pueued` (https://github.com/Nukesor/pueue) is a task queue daemon for a single machine. The
handler adds the resolved `custom_command` as a task and reads the task's state back from
`pueue status --json`. Like the docker engine this one supports the custom software type only.

The job id is the pueue task id. The task runs in the job's output folder and redirects its
output to `LOG_FILE_NAME` there. Pueue copies the environment of the shell that adds a task, so the
environment variables are exported in the same SSH command.

Precondition: `custom_command` contains only `a-zA-Z0-9-_+./` and spaces (enforced by the webapp
and by `_check_content` in the processor), so it never needs escaping.

Notes:
    - requires `pueued` running on the machine under the SSH user, and `pueue` on the `PATH` of a
      non-interactive SSH session. Tested against the JSON schema of pueue 4.x.
    - the number of parallel jobs on the machine is set with `pueue parallel <n>` on the
      machine; `_SLURM_MEM`, `_SLURM_CPUS_PER_TASK` and `_SLURM_TIME` are not honored.
    - a task removed from pueue (e.g. by `pueue clean`) is reported as `UNKNOWN`.
    - on windows, pueue runs tasks in Windows PowerShell, whose `>` redirection writes UTF-16.

"""

import base64
import json
import logging
from datetime import datetime
from pathlib import PurePath
from typing import Any

from airflow.exceptions import AirflowFailException
from common.constants import LOG_FILE_NAME
from common.keys import JobStates
from common.quanting_env import QuantingEnv
from jobs.job_handler import JobHandler, posix_export_lines
from sensors.ssh_utils import ssh_execute

from shared.runners import OperatingSystems

PUEUE_EXE = "pueue"
STATUS_CMD = f"{PUEUE_EXE} status --json"

# pueue's `TaskStatus` variants that are not `Done`
_PUEUE_STATUS_TO_JOB_STATE = {
    "Stashed": JobStates.PENDING,
    "Queued": JobStates.PENDING,
    "Locked": JobStates.PENDING,
    "Running": JobStates.RUNNING,
    "Paused": JobStates.RUNNING,
}
_DONE = "Done"
_SUCCESS = "Success"


def _add_cmd(output_path: PurePath, custom_command: str, label: str) -> str:
    """The `pueue add` command, printing only the task id."""
    return (
        f'{PUEUE_EXE} add --print-task-id --working-directory "{output_path}" --label "{label}" '
        f'-- "{custom_command} > {LOG_FILE_NAME} 2>&1"'
    )


def _posix_start_cmd(
    environment: dict[str, str], output_path: PurePath, custom_command: str, label: str
) -> str:
    return "\n".join(
        [
            *posix_export_lines(environment),
            _add_cmd(output_path, custom_command, label),
        ]
    )


def _windows_start_cmd(
    environment: dict[str, str], output_path: PurePath, custom_command: str, label: str
) -> str:
    """Run the start command in PowerShell whatever the default shell of the OpenSSH server is.

    `-EncodedCommand` takes the script as base64 of UTF-16LE, so neither cmd.exe, PowerShell nor
    git-bash get to interpret any character of it.
    """
    script = "; ".join(
        [
            *[f"$env:{key} = '{value}'" for key, value in environment.items()],
            _add_cmd(output_path, custom_command, label),
        ]
    )
    logging.info(f"Encoding powershell script: >>>>\n{script}\n<<<< end of script")
    encoded = base64.b64encode(script.encode("utf-16-le")).decode("ascii")
    return f"powershell -NoProfile -EncodedCommand {encoded}"


_OS_TO_START_CMD = {
    OperatingSystems.LINUX: _posix_start_cmd,
    OperatingSystems.MACOS: _posix_start_cmd,
    OperatingSystems.WINDOWS: _windows_start_cmd,
}


class PueueSSHJobHandler(JobHandler):
    """Implementation of JobHandler that queues jobs with pueue via SSH."""

    def __init__(
        self, output_dir: PurePath, runner_os: str, ssh_connection_id_prefix: str
    ):
        """Initialize the pueue job handler.

        Args:
            output_dir: The output location as seen from the runner
            runner_os: The operating system of the runner, cf. `OperatingSystems`
            ssh_connection_id_prefix: Prefix of the Airflow connections to the runner

        """
        super().__init__()
        self._output_dir = output_dir
        self._start_cmd = _OS_TO_START_CMD[runner_os]
        self._ssh_connection_id_prefix = ssh_connection_id_prefix

    def start_job(self, quanting_env: QuantingEnv) -> str:
        """Add the job as a pueue task via SSH.

        Returns:
            Job ID (in the case of this handler, the pueue task id)

        """
        command = self._start_cmd(
            quanting_env.to_exportable_dict(),
            self._output_dir / quanting_env.relative_output_path,
            quanting_env.custom_command,
            quanting_env.raw_file_id,
        )
        logging.info(f"Running command: >>>>\n{command}\n<<<< end of command")
        ssh_return = ssh_execute(command, self._ssh_connection_id_prefix)

        try:
            return str(int(ssh_return.split("\n")[-1]))
        except ValueError as e:
            raise AirflowFailException(
                f"Did not get a task id from pueue: {ssh_return!r}"
            ) from e

    def get_job_status(self, job_id: str) -> str:
        """Get the status of a job from pueue via SSH."""
        return self.get_job_result(job_id)[0]

    def get_job_result(self, job_id: str) -> tuple[str, int]:
        """Get the job status and the elapsed seconds from pueue via SSH."""
        task = self._get_task(job_id)
        if task is None:
            logging.warning(f"Task {job_id} not found in pueue.")
            return JobStates.UNKNOWN, 0

        variant, details = _untag(task["status"])
        return _to_job_state(variant, details), _get_time_elapsed(details)

    def _get_task(self, job_id: str) -> dict[str, Any] | None:
        """Get the pueue task with the given id, None if pueue does not know it (anymore)."""
        ssh_return = ssh_execute(STATUS_CMD, self._ssh_connection_id_prefix)
        try:
            state = json.loads(ssh_return)
        except json.JSONDecodeError as e:
            raise AirflowFailException(
                f"Did not get valid json from pueue: {ssh_return[:200]!r}"
            ) from e

        return state["tasks"].get(job_id)


def _untag(tagged: str | dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """Split a serde externally tagged enum value, `"Variant"` or `{"Variant": {...}}`, into variant and details."""
    if isinstance(tagged, str):
        return tagged, {}
    ((variant, details),) = tagged.items()
    return variant, details if isinstance(details, dict) else {}


def _to_job_state(variant: str, details: dict[str, Any]) -> str:
    """Map a pueue task status to a job state."""
    if variant != _DONE:
        return _PUEUE_STATUS_TO_JOB_STATE.get(variant, JobStates.UNKNOWN)

    result = details.get("result")
    if result == _SUCCESS:
        return JobStates.COMPLETED

    logging.info(f"Task finished with {result=}")
    return JobStates.FAILED


def _get_time_elapsed(details: dict[str, Any]) -> int:
    """Get the seconds between the `start` and `end` timestamps of a task status, 0 if not (yet) available."""
    start, end = details.get("start"), details.get("end")
    if not start or not end:
        return 0

    return int(
        (datetime.fromisoformat(end) - datetime.fromisoformat(start)).total_seconds()
    )
