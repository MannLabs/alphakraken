"""Tests for the pueue_job_handler module."""

import base64
import json
from collections.abc import Callable
from pathlib import PurePosixPath, PureWindowsPath
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowFailException
from common.constants import LOG_FILE_NAME
from common.keys import JobStates
from common.quanting_env import QuantingEnv
from jobs.pueue_job_handler import STATUS_CMD, PueueJobHandler

from shared.keys import SoftwareTypes
from shared.runners import OperatingSystems

MODULE = "jobs.pueue_job_handler"

SSH_PREFIX = "box_ssh"
RELATIVE_OUTPUT_PATH = "P1/out_raw_file_1.raw/custom"
CUSTOM_COMMAND = "/runner/software/run_msqc.sh --threads 2"
JOB_ID = "42"

POSIX_OUTPUT_DIR = PurePosixPath("/runner/output")
WINDOWS_OUTPUT_DIR = PureWindowsPath(r"Z:\alphakraken\output")

START = "2025-09-09T14:00:00.123456789+02:00"
END = "2025-09-09T14:08:42.987654321+02:00"


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


def _handler(runner_os: str = OperatingSystems.LINUX) -> PueueJobHandler:
    output_dir = (
        WINDOWS_OUTPUT_DIR
        if runner_os == OperatingSystems.WINDOWS
        else POSIX_OUTPUT_DIR
    )
    return PueueJobHandler(output_dir, runner_os, SSH_PREFIX)


def _status_json(status: str | dict, job_id: str = JOB_ID) -> str:
    """Build the `pueue status --json` output for one task with the given `status` variant."""
    return json.dumps(
        {"tasks": {job_id: {"id": int(job_id), "status": status}}, "groups": {}}
    )


def _done(result: str | dict) -> dict:
    return {
        "Done": {"enqueued_at": START, "start": START, "end": END, "result": result}
    }


@patch(f"{MODULE}.ssh_execute")
class TestStartJob:
    """Test cases for PueueJobHandler.start_job()."""

    def test_posix_start_exports_env_and_adds_task_in_output_folder(
        self, mock_ssh_execute: MagicMock, sample_quanting_env: QuantingEnv
    ) -> None:
        """Test that the env is exported before `pueue add`, which runs the command in the output folder."""
        mock_ssh_execute.return_value = "42"

        # when
        job_id = _handler().start_job(sample_quanting_env)

        assert job_id == JOB_ID
        assert mock_ssh_execute.call_args.args[1] == SSH_PREFIX
        lines = mock_ssh_execute.call_args.args[0].split("\n")
        assert (
            'export RAW_FILE_PATH="/pool/backup/instrument1/1970_01/test_file.raw"'
            in lines
        )
        assert not [line for line in lines if line.startswith("export _")]
        assert lines[-1] == (
            "pueue add --print-task-id "
            '--working-directory "/runner/output/P1/out_raw_file_1.raw/custom" '
            '--label "raw_file_1.raw" '
            f'-- "{CUSTOM_COMMAND} > {LOG_FILE_NAME} 2>&1"'
        )

    def test_every_exported_key_is_present(
        self, mock_ssh_execute: MagicMock, sample_quanting_env: QuantingEnv
    ) -> None:
        """Test that every non-underscore key of the quanting env is exported."""
        mock_ssh_execute.return_value = "42"
        expected_keys = {
            k for k in sample_quanting_env.to_dict() if not k.startswith("_")
        }

        _handler().start_job(sample_quanting_env)

        lines = mock_ssh_execute.call_args.args[0].split("\n")
        exported = {line.split("=")[0].removeprefix("export ") for line in lines[:-1]}
        assert exported == expected_keys

    def test_windows_start_is_an_encoded_powershell_script(
        self, mock_ssh_execute: MagicMock, sample_quanting_env: QuantingEnv
    ) -> None:
        """Test that the windows start command is a base64 UTF-16LE script setting the env and adding the task."""
        mock_ssh_execute.return_value = "42"

        # when
        job_id = _handler(OperatingSystems.WINDOWS).start_job(sample_quanting_env)

        assert job_id == JOB_ID
        command = mock_ssh_execute.call_args.args[0]
        prefix = "powershell -NoProfile -EncodedCommand "
        assert command.startswith(prefix)
        script = base64.b64decode(command.removeprefix(prefix)).decode("utf-16-le")
        statements = script.split("; ")
        assert (
            "$env:RAW_FILE_PATH = '/pool/backup/instrument1/1970_01/test_file.raw'"
            in statements
        )
        assert not [s for s in statements if s.startswith("$env:_")]
        assert statements[-1] == (
            "pueue add --print-task-id "
            r'--working-directory "Z:\alphakraken\output\P1\out_raw_file_1.raw\custom" '
            '--label "raw_file_1.raw" '
            f'-- "{CUSTOM_COMMAND} > {LOG_FILE_NAME} 2>&1"'
        )

    def test_start_job_takes_the_last_line_as_task_id(
        self, mock_ssh_execute: MagicMock, sample_quanting_env: QuantingEnv
    ) -> None:
        """Test that noise before the task id is ignored."""
        mock_ssh_execute.return_value = "some noise\n42"

        assert _handler().start_job(sample_quanting_env) == JOB_ID

    def test_start_job_raises_on_non_numeric_task_id(
        self, mock_ssh_execute: MagicMock, sample_quanting_env: QuantingEnv
    ) -> None:
        """Test that a last stdout line that is not a task id fails the job."""
        mock_ssh_execute.return_value = "pueue: command not found"

        with pytest.raises(AirflowFailException, match="command not found"):
            _handler().start_job(sample_quanting_env)


@patch(f"{MODULE}.ssh_execute")
class TestJobStatus:
    """Test cases for get_job_status() and get_job_result()."""

    @pytest.mark.parametrize(
        ("status", "expected"),
        [
            ({"Queued": {"enqueued_at": START}}, (JobStates.PENDING, 0)),
            ({"Stashed": {"enqueue_at": None}}, (JobStates.PENDING, 0)),
            ({"Locked": {"previous_status": "Queued"}}, (JobStates.PENDING, 0)),
            (
                {"Running": {"enqueued_at": START, "start": START}},
                (JobStates.RUNNING, 0),
            ),
            (
                {"Paused": {"enqueued_at": START, "start": START}},
                (JobStates.RUNNING, 0),
            ),
            (_done("Success"), (JobStates.COMPLETED, 522)),
            (_done({"Failed": 3}), (JobStates.FAILED, 522)),
            (_done({"FailedToSpawn": "not found"}), (JobStates.FAILED, 522)),
            (_done("Killed"), (JobStates.FAILED, 522)),
            (_done("Errored"), (JobStates.FAILED, 522)),
            (_done("DependencyFailed"), (JobStates.FAILED, 522)),
            ({"Frobnicating": {}}, (JobStates.UNKNOWN, 0)),
        ],
    )
    def test_status_and_result_map_the_pueue_task_status(
        self, mock_ssh_execute: MagicMock, status: dict, expected: tuple[str, int]
    ) -> None:
        """Test that the externally tagged pueue status maps to a job state and elapsed seconds."""
        mock_ssh_execute.return_value = _status_json(status)
        handler = _handler()

        assert handler.get_job_status(JOB_ID) == expected[0]
        assert handler.get_job_result(JOB_ID) == expected
        mock_ssh_execute.assert_called_with(STATUS_CMD, SSH_PREFIX)

    def test_unknown_task_is_reported_as_unknown(
        self, mock_ssh_execute: MagicMock
    ) -> None:
        """Test that a task pueue does not know (anymore) yields UNKNOWN, like a removed container does."""
        mock_ssh_execute.return_value = _status_json(_done("Success"), job_id="7")

        assert _handler().get_job_result(JOB_ID) == (JobStates.UNKNOWN, 0)

    def test_invalid_json_raises(self, mock_ssh_execute: MagicMock) -> None:
        """Test that a non-json response is not mistaken for a state."""
        mock_ssh_execute.return_value = "pueue: command not found"

        with pytest.raises(AirflowFailException, match="valid json"):
            _handler().get_job_status(JOB_ID)
