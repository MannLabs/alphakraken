"""Implementation of a job handler that runs jobs as background processes on a machine reachable via SSH.

No scheduler, no container: the resolved `custom_command` is run directly on the remote machine,
so like the docker engine this one supports the custom software type only.

The handler writes a launcher script into the job's output folder (which the worker and the runner
share) and uses SSH only to start it and to inspect it. The launcher captures the command's exit
code and elapsed seconds into `EXIT_CODE_FILE_NAME`, the job's output into `LOG_FILE_NAME`. The
job id is `<process id of the launcher>:<relative output path>`, as the status commands need both.

Precondition: `custom_command` contains only `a-zA-Z0-9-_+./` and spaces (enforced by the webapp
and by `_check_content` in the processor), so it never needs escaping in the launcher.

Notes:
    - the same folder must be reachable as `AIRFLOW_CONTAINER_VIEW`'s `output` on the worker and
      as the runner's `view.output`, which the pipeline already assumes for reading the results.
    - `_SLURM_MEM`, `_SLURM_CPUS_PER_TASK` and `_SLURM_TIME` are not honored: there is no resource
      control, concurrency is bounded by `cluster_slots_pool` only.
    - on windows, `time_elapsed` is always 0.
    - if the launcher dies before writing the exit code file and its pid is recycled, the job
      stays `RUNNING`.

"""

import base64
import logging
from pathlib import PurePath
from typing import Protocol

from airflow.exceptions import AirflowFailException
from common.constants import EXIT_CODE_FILE_NAME, LAUNCHER_SCRIPT_STEM, LOG_FILE_NAME
from common.keys import JobStates
from common.quanting_env import QuantingEnv
from jobs.job_handler import JobHandler, posix_export_lines
from sensors.ssh_utils import ssh_execute

from shared.path_views import AIRFLOW_CONTAINER_VIEW, Locations
from shared.runners import OperatingSystems

JOB_ID_SEPARATOR = ":"  # cannot occur in a relative output path, cf. shared.validation

_STATUS_STATES = (JobStates.COMPLETED, JobStates.FAILED, JobStates.RUNNING)


class _Dialect(Protocol):
    """The OS-specific parts: the launcher script and the two commands to start and inspect it.

    Every command prints exactly one line, as `ssh_execute` retries on empty output.
    """

    launcher_file_name: str

    def launcher_script(
        self, environment: dict[str, str], output_path: PurePath, custom_command: str
    ) -> str:
        """Content of the launcher script, which writes `<exit code> <elapsed seconds>` to the exit code file."""
        ...

    def start_cmd(self, launcher_path: PurePath) -> str:
        """Command to start the launcher in the background, printing its process id."""
        ...

    def status_cmd(self, exit_code_file_path: PurePath, job_id: str) -> str:
        """Command printing `<STATE> <ELAPSED_SECONDS>`, the state being one of `_STATUS_STATES`."""
        ...


class _PosixDialect:
    """Dialect for linux and macos, using `sh`."""

    launcher_file_name = f"{LAUNCHER_SCRIPT_STEM}.sh"

    def launcher_script(
        self, environment: dict[str, str], output_path: PurePath, custom_command: str
    ) -> str:
        """Content of the launcher script."""
        lines = [
            *posix_export_lines(environment),
            f'cd "{output_path}"',
            "START=$(date +%s)",
            f"{custom_command} > {LOG_FILE_NAME} 2>&1",
            "CODE=$?",
            f'echo "$CODE $(($(date +%s) - START))" > {EXIT_CODE_FILE_NAME}',
        ]
        return "\n".join(lines) + "\n"

    def start_cmd(self, launcher_path: PurePath) -> str:
        """Command to start the launcher in the background."""
        return f'nohup sh "{launcher_path}" > /dev/null 2>&1 &\necho $!'

    def status_cmd(self, exit_code_file_path: PurePath, job_id: str) -> str:
        """Command printing the job state and the elapsed seconds."""
        return "\n".join(
            [
                f'if [ -f "{exit_code_file_path}" ]; then',
                f'  read CODE ELAPSED < "{exit_code_file_path}"',
                f'  [ "$CODE" = 0 ] && echo "{JobStates.COMPLETED} $ELAPSED" || echo "{JobStates.FAILED} $ELAPSED"',
                f'elif kill -0 {job_id} 2>/dev/null; then echo "{JobStates.RUNNING} 0"',
                f'else echo "{JobStates.FAILED} 0"; fi',
            ]
        )


class _WindowsDialect:
    """Dialect for windows, using a `.cmd` launcher and powershell to start and inspect it.

    `powershell -NoProfile -Command "..."` works whatever the default shell of the OpenSSH server
    is (cmd.exe, powershell or git-bash), as long as the script contains no `"`, `$`, `%` or
    backtick: those are interpreted differently by the three. The start command needs `"` around
    the launcher path, so it goes through `-EncodedCommand` instead.
    Batch files are written with CRLF line endings, cmd.exe misparses LF-only files in some cases.

    The launcher is spawned by WMI, not by `Start-Process`: `ssh-shellhost.exe` puts the session in
    a job object with "kill on job close" and without "breakaway ok", so anything started within the
    session dies when the SSH command returns. A process
    created via `Win32_Process` is a child of `WmiPrvSE.exe` and outside that job object.
    """

    launcher_file_name = f"{LAUNCHER_SCRIPT_STEM}.cmd"

    def launcher_script(
        self, environment: dict[str, str], output_path: PurePath, custom_command: str
    ) -> str:
        """Content of the launcher script; the elapsed time is always 0."""
        lines = [
            "@echo off",
            *[f'set "{key}={value}"' for key, value in environment.items()],
            f'cd /d "{output_path}"',
            f"call {custom_command} > {LOG_FILE_NAME} 2>&1",
            f"echo %ERRORLEVEL% 0 > {EXIT_CODE_FILE_NAME}",
        ]
        return "\r\n".join(lines) + "\r\n"

    def start_cmd(self, launcher_path: PurePath) -> str:
        """Command to start the launcher outside the SSH session's job object."""
        script = (
            "(Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments "
            f"""@{{CommandLine = 'cmd.exe /c call "{launcher_path}"'}}).ProcessId"""
        )
        encoded = base64.b64encode(script.encode("utf-16-le")).decode("ascii")
        return f"powershell -NoProfile -EncodedCommand {encoded}"

    def status_cmd(self, exit_code_file_path: PurePath, job_id: str) -> str:
        """Command printing the job state and the elapsed seconds.

        The exit code file holds `<code> 0`; the `-replace` chain turns the code into the state.
        """
        return self._powershell(
            f"if (Test-Path -LiteralPath '{exit_code_file_path}') "
            f"{{ (Get-Content -LiteralPath '{exit_code_file_path}' -First 1).Trim() "
            f"-replace '^0 ', '{JobStates.COMPLETED} ' -replace '^-?[1-9][0-9]* ', '{JobStates.FAILED} ' }} "
            f"elseif (Get-Process -Id {job_id} -ErrorAction SilentlyContinue) {{ '{JobStates.RUNNING} 0' }} "
            f"else {{ '{JobStates.FAILED} 0' }}"
        )

    @staticmethod
    def _powershell(script: str) -> str:
        return f'powershell -NoProfile -Command "{script}"'


_OS_TO_DIALECT: dict[str, _Dialect] = {
    OperatingSystems.LINUX: _PosixDialect(),
    OperatingSystems.MACOS: _PosixDialect(),
    OperatingSystems.WINDOWS: _WindowsDialect(),
}


class SimpleSSHJobHandler(JobHandler):
    """Implementation of JobHandler that runs jobs as background processes via SSH."""

    def __init__(
        self, output_dir: PurePath, runner_os: str, ssh_connection_id_prefix: str
    ):
        """Initialize the direct SSH job handler.

        Args:
            output_dir: The output location as seen from the runner
            runner_os: The operating system of the runner, cf. `OperatingSystems`
            ssh_connection_id_prefix: Prefix of the Airflow connections to the runner

        """
        super().__init__()
        self._output_dir = output_dir
        self._dialect = _OS_TO_DIALECT[runner_os]
        self._ssh_connection_id_prefix = ssh_connection_id_prefix

    def start_job(self, quanting_env: QuantingEnv) -> str:
        """Write the launcher script into the output folder and start it via SSH.

        Returns:
            Job ID (in the case of this handler, the process id of the launcher)

        """
        local_output_path = AIRFLOW_CONTAINER_VIEW.resolve(
            Locations.OUTPUT, quanting_env.relative_output_path
        )
        if not local_output_path.exists():
            raise AirflowFailException(
                f"Path {local_output_path} does not exist in the worker."
            )
        remote_output_path = self._output_dir / quanting_env.relative_output_path

        script = self._dialect.launcher_script(
            quanting_env.to_exportable_dict(),
            remote_output_path,
            quanting_env.custom_command,
        )
        launcher_path = local_output_path / self._dialect.launcher_file_name
        # newline="" keeps the dialect's line endings
        launcher_path.write_text(script, newline="")
        logging.info(
            f"Wrote launcher {launcher_path}: >>>>\n{script}<<<< end of launcher"
        )

        command = self._dialect.start_cmd(
            remote_output_path / self._dialect.launcher_file_name
        )
        logging.info(f"Running command: >>>>\n{command}\n<<<< end of command")
        ssh_return = ssh_execute(command, self._ssh_connection_id_prefix)

        try:
            pid = int(ssh_return.split("\n")[-1])
        except ValueError as e:
            raise AirflowFailException(
                f"Did not get a process id from the runner: {ssh_return!r}"
            ) from e

        return f"{pid}{JOB_ID_SEPARATOR}{quanting_env.relative_output_path}"

    def get_job_status(self, job_id: str) -> str:
        """Get the status of a job via SSH."""
        return self._get_status_line(job_id)[0]

    def get_job_result(self, job_id: str) -> tuple[str, int]:
        """Get the job status and the elapsed seconds via SSH."""
        return self._get_status_line(job_id)

    def _get_status_line(self, job_id: str) -> tuple[str, int]:
        """Run the status command and parse its `<STATE> <ELAPSED_SECONDS>` line."""
        pid, relative_output_path = job_id.split(JOB_ID_SEPARATOR, 1)
        command = self._dialect.status_cmd(
            self._output_dir / relative_output_path / EXIT_CODE_FILE_NAME,
            str(int(pid)),  # the pid ends up in a shell command
        )
        ssh_return = ssh_execute(command, self._ssh_connection_id_prefix)

        tokens = ssh_return.split("\n")[-1].split()
        try:
            state, elapsed = tokens
            if state not in _STATUS_STATES:
                raise ValueError(state)  # noqa: TRY301
            return state, int(elapsed)
        except ValueError as e:
            raise AirflowFailException(
                f"Unexpected status line from the runner: {ssh_return!r}"
            ) from e
