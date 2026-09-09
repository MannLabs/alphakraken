"""Tests for the simple_ssh_job_handler module."""

import base64
import re
from collections.abc import Callable, Iterator
from pathlib import Path, PurePosixPath, PureWindowsPath
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowFailException
from common.constants import EXIT_CODE_FILE_NAME, LOG_FILE_NAME
from common.keys import JobStates
from common.quanting_env import QuantingEnv
from jobs.simple_ssh_job_handler import (
    SimpleSSHJobHandler,
    _PosixDialect,
    _WindowsDialect,
)

from shared.keys import SoftwareTypes
from shared.path_views import Locations, View
from shared.runners import OperatingSystems
from shared.validation import check_for_malicious_content

MODULE = "jobs.simple_ssh_job_handler"

SSH_PREFIX = "box_ssh"
RELATIVE_OUTPUT_PATH = "P1/out_raw_file_1.raw/custom"
CUSTOM_COMMAND = "/runner/software/run_msqc.sh --threads 2"
JOB_ID = f"4711:{RELATIVE_OUTPUT_PATH}"

POSIX_OUTPUT_DIR = PurePosixPath("/runner/output")
WINDOWS_OUTPUT_DIR = PureWindowsPath(r"Z:\alphakraken\output")


@pytest.fixture
def sample_quanting_env(
    make_quanting_env: Callable[..., QuantingEnv],
) -> QuantingEnv:
    """Create a quanting environment for a custom software job."""
    return make_quanting_env(
        raw_file_id="raw_file_1.raw",
        software="run_msqc.sh",
        software_type=SoftwareTypes.CUSTOM,
        custom_command=CUSTOM_COMMAND,
        relative_output_path=RELATIVE_OUTPUT_PATH,
    )


@pytest.fixture
def local_output_path(tmp_path: Path) -> Iterator[Path]:
    """Create the job's output folder in a worker view rooted at `tmp_path`."""
    view = View("test", {Locations.OUTPUT: str(tmp_path)}, Path)
    output_path = tmp_path / RELATIVE_OUTPUT_PATH
    output_path.mkdir(parents=True)
    with patch(f"{MODULE}.AIRFLOW_CONTAINER_VIEW", view):
        yield output_path


def _decoded(command: str) -> str:
    """Get the script back from a `-EncodedCommand` powershell call."""
    return base64.b64decode(command.rsplit(" ", 1)[-1]).decode("utf-16-le")


def _handler(runner_os: str = OperatingSystems.LINUX) -> SimpleSSHJobHandler:
    output_dir = (
        WINDOWS_OUTPUT_DIR
        if runner_os == OperatingSystems.WINDOWS
        else POSIX_OUTPUT_DIR
    )
    return SimpleSSHJobHandler(output_dir, runner_os, SSH_PREFIX)


def _environment(quanting_env: QuantingEnv) -> dict[str, str]:
    return {
        k: str(v) for k, v in quanting_env.to_dict().items() if not k.startswith("_")
    }


class TestLauncherScript:
    """Test cases for the launcher scripts of both dialects."""

    def test_posix_launcher_exports_env_and_captures_exit_code(
        self, sample_quanting_env: QuantingEnv
    ) -> None:
        """Test that the posix launcher has the exports, the command and the exit code capture."""
        # when
        script = _PosixDialect().launcher_script(
            _environment(sample_quanting_env),
            POSIX_OUTPUT_DIR / RELATIVE_OUTPUT_PATH,
            CUSTOM_COMMAND,
        )

        lines = script.splitlines()
        assert (
            'export RAW_FILE_PATH="/pool/backup/instrument1/1970_01/test_file.raw"'
            in lines
        )
        assert 'export SOFTWARE_TYPE="custom"' in lines
        assert not [line for line in lines if line.startswith("export _")]
        assert 'cd "/runner/output/P1/out_raw_file_1.raw/custom"' in lines
        assert f"{CUSTOM_COMMAND} > {LOG_FILE_NAME} 2>&1" in lines
        assert (
            lines[-1]
            == f'echo "$CODE $(($(date +%s) - START))" > {EXIT_CODE_FILE_NAME}'
        )

    def test_windows_launcher_sets_env_and_captures_exit_code(
        self, sample_quanting_env: QuantingEnv
    ) -> None:
        """Test that the windows launcher has the sets, the command and the exit code capture, with CRLF."""
        # when
        script = _WindowsDialect().launcher_script(
            _environment(sample_quanting_env),
            WINDOWS_OUTPUT_DIR / RELATIVE_OUTPUT_PATH,
            CUSTOM_COMMAND,
        )

        assert "\r\n" in script
        assert "\n" not in script.replace("\r\n", "")
        lines = script.splitlines()
        assert lines[0] == "@echo off"
        assert (
            'set "RAW_FILE_PATH=/pool/backup/instrument1/1970_01/test_file.raw"'
            in lines
        )
        assert not [line for line in lines if line.startswith('set "_')]
        assert r'cd /d "Z:\alphakraken\output\P1\out_raw_file_1.raw\custom"' in lines
        assert f"call {CUSTOM_COMMAND} > {LOG_FILE_NAME} 2>&1" in lines
        assert lines[-1] == f"echo %ERRORLEVEL% 0 > {EXIT_CODE_FILE_NAME}"

    def test_every_exported_key_is_present(
        self, sample_quanting_env: QuantingEnv
    ) -> None:
        """Test that every non-underscore key of the quanting env is set by both launchers."""
        environment = _environment(sample_quanting_env)
        expected_keys = {
            k for k in sample_quanting_env.to_dict() if not k.startswith("_")
        }
        assert expected_keys  # sanity

        posix = _PosixDialect().launcher_script(environment, POSIX_OUTPUT_DIR, "cmd")
        windows = _WindowsDialect().launcher_script(
            environment, WINDOWS_OUTPUT_DIR, "cmd"
        )

        assert set(re.findall(r"^export (\w+)=", posix, re.MULTILINE)) == expected_keys
        assert set(re.findall(r'^set "(\w+)=', windows, re.MULTILINE)) == expected_keys


@pytest.mark.parametrize(
    "runner_os",
    [OperatingSystems.LINUX, OperatingSystems.MACOS, OperatingSystems.WINDOWS],
)
class TestCommandsPrintOutput:
    """Every command must print something, as `ssh_execute` retries on empty output."""

    def test_start_cmd_prints_the_pid(self, runner_os: str) -> None:
        """Test that the start command ends in printing the process id."""
        cmd = _handler(runner_os)._dialect.start_cmd(PurePosixPath("/x/launcher"))

        if "-EncodedCommand" in cmd:
            cmd = _decoded(cmd)
        assert cmd.endswith(("echo $!", ").ProcessId"))

    def test_status_cmd_prints_a_state_on_every_branch(self, runner_os: str) -> None:
        """Test that every branch of the status command prints a state."""
        cmd = _handler(runner_os)._dialect.status_cmd(PurePosixPath("/x/exit"), "4711")

        assert cmd.count(JobStates.COMPLETED) == 1
        assert cmd.count(JobStates.RUNNING) == 1
        assert cmd.count(JobStates.FAILED) == 2  # nonzero exit code, and dead process


@patch(f"{MODULE}.ssh_execute")
class TestStartJob:
    """Test cases for SimpleSSHJobHandler.start_job()."""

    def test_start_job_writes_launcher_and_returns_pid_with_output_path(
        self,
        mock_ssh_execute: MagicMock,
        sample_quanting_env: QuantingEnv,
        local_output_path: Path,
    ) -> None:
        """Test that the launcher lands in the output folder and the job id carries pid and folder."""
        mock_ssh_execute.return_value = "some noise\n4711"

        # when
        job_id = _handler().start_job(sample_quanting_env)

        assert job_id == JOB_ID
        launcher = local_output_path / "_alphakraken_job.sh"
        assert launcher.exists()
        assert CUSTOM_COMMAND in launcher.read_text()

        command = mock_ssh_execute.call_args.args[0]
        assert mock_ssh_execute.call_args.args[1] == SSH_PREFIX
        assert command == (
            'nohup sh "/runner/output/P1/out_raw_file_1.raw/custom/_alphakraken_job.sh" '
            "> /dev/null 2>&1 &\necho $!"
        )

    def test_start_job_windows_writes_cmd_launcher_with_crlf(
        self,
        mock_ssh_execute: MagicMock,
        sample_quanting_env: QuantingEnv,
        local_output_path: Path,
    ) -> None:
        """Test that a windows runner gets a `.cmd` launcher with CRLF preserved on disk."""
        mock_ssh_execute.return_value = "4711"

        # when
        job_id = _handler(OperatingSystems.WINDOWS).start_job(sample_quanting_env)

        assert job_id == JOB_ID
        launcher = local_output_path / "_alphakraken_job.cmd"
        assert b"\r\n" in launcher.read_bytes()

        command = mock_ssh_execute.call_args.args[0]
        assert command.startswith("powershell -NoProfile -EncodedCommand ")
        assert _decoded(command) == (
            "(Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments "
            r"""@{CommandLine = 'cmd.exe /c call "Z:\alphakraken\output\P1\out_raw_file_1.raw"""
            r"""\custom\_alphakraken_job.cmd"'}).ProcessId"""
        )

    def test_start_job_raises_on_non_numeric_pid(
        self,
        mock_ssh_execute: MagicMock,
        sample_quanting_env: QuantingEnv,
        local_output_path: Path,  # noqa: ARG002
    ) -> None:
        """Test that a last stdout line that is not a process id fails the job."""
        mock_ssh_execute.return_value = "sh: not found"

        with pytest.raises(AirflowFailException, match="sh: not found"):
            _handler().start_job(sample_quanting_env)

    def test_start_job_raises_if_output_folder_is_missing(
        self,
        mock_ssh_execute: MagicMock,
        sample_quanting_env: QuantingEnv,
        tmp_path: Path,
    ) -> None:
        """Test that a missing local output folder is reported by path, before any SSH call."""
        view = View("test", {Locations.OUTPUT: str(tmp_path)}, Path)

        with (
            patch(f"{MODULE}.AIRFLOW_CONTAINER_VIEW", view),
            pytest.raises(AirflowFailException, match=re.escape(RELATIVE_OUTPUT_PATH)),
        ):
            _handler().start_job(sample_quanting_env)

        mock_ssh_execute.assert_not_called()


@patch(f"{MODULE}.ssh_execute")
class TestJobStatus:
    """Test cases for get_job_status() and get_job_result()."""

    @pytest.mark.parametrize(
        ("line", "expected"),
        [
            ("COMPLETED 42", (JobStates.COMPLETED, 42)),
            ("FAILED 7", (JobStates.FAILED, 7)),
            ("RUNNING 0", (JobStates.RUNNING, 0)),
        ],
    )
    def test_status_and_result_parse_the_status_line(
        self, mock_ssh_execute: MagicMock, line: str, expected: tuple[str, int]
    ) -> None:
        """Test that the state is the first token and the elapsed seconds the second."""
        mock_ssh_execute.return_value = line
        handler = _handler()

        assert handler.get_job_status(JOB_ID) == expected[0]
        assert handler.get_job_result(JOB_ID) == expected

    @pytest.mark.parametrize("line", ["", "garbage", "DONE 1", "COMPLETED soon"])
    def test_garbage_status_line_raises(
        self, mock_ssh_execute: MagicMock, line: str
    ) -> None:
        """Test that a line outside the contract is not mistaken for a state."""
        mock_ssh_execute.return_value = line

        with pytest.raises(AirflowFailException, match="Unexpected status line"):
            _handler().get_job_status(JOB_ID)

    def test_status_cmd_points_to_exit_code_file_and_pid(
        self, mock_ssh_execute: MagicMock
    ) -> None:
        """Test that the status command is built from the job id's two parts."""
        mock_ssh_execute.return_value = "RUNNING 0"

        # when
        _handler().get_job_status(JOB_ID)

        command = mock_ssh_execute.call_args.args[0]
        assert (
            f'"/runner/output/{RELATIVE_OUTPUT_PATH}/{EXIT_CODE_FILE_NAME}"' in command
        )
        assert "kill -0 4711 " in command

    def test_windows_status_cmd_points_to_exit_code_file_and_pid(
        self, mock_ssh_execute: MagicMock
    ) -> None:
        """Test that the windows status command uses the windows path flavour."""
        mock_ssh_execute.return_value = "RUNNING 0"

        # when
        _handler(OperatingSystems.WINDOWS).get_job_status(JOB_ID)

        command = mock_ssh_execute.call_args.args[0]
        assert command.startswith('powershell -NoProfile -Command "')
        assert (
            rf"'Z:\alphakraken\output\P1\out_raw_file_1.raw\custom\{EXIT_CODE_FILE_NAME}'"
            in command
        )
        assert "Get-Process -Id 4711 " in command
        # these break the command in one of the three possible default shells
        assert not set(command[len('powershell -NoProfile -Command "') : -1]) & set(
            '"$%`'
        )

    def test_non_numeric_pid_in_job_id_raises(
        self, mock_ssh_execute: MagicMock
    ) -> None:
        """Test that a tampered job id never reaches the shell."""
        with pytest.raises(ValueError, match="rm -rf"):
            _handler().get_job_status(f"rm -rf:{RELATIVE_OUTPUT_PATH}")

        mock_ssh_execute.assert_not_called()


def test_config_params_cannot_contain_shell_metacharacters() -> None:
    """The launcher quotes nothing: the validator of `config_params` must keep the shell's metacharacters out."""
    assert (
        check_for_malicious_content("--threads 2 --in x_1.raw", allow_spaces=True) == []
    )

    for bad in ['"', "'", ";", "$", "`", "&", "|", ">", "\n"]:
        assert check_for_malicious_content(f"cmd {bad}x", allow_spaces=True), bad
