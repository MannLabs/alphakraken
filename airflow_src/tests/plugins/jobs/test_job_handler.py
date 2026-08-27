"""Tests for the job_handler module."""

import sys
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowFailException
from jobs._experimental.file_based_job_handler import FileBasedJobHandler
from jobs._experimental.generic_job_handler import GenericJobHandler
from jobs.job_handler import (
    SlurmNoSacctSSHJobHandler,
    SlurmSSHJobHandler,
    _get_job_handler,
)

from airflow_src.tests.helpers import yaml_locations
from shared.keys import JobEngines


@yaml_locations(slurm="/path/to/slurm_base_path")
def test_get_job_handler_routes_engine_to_handler() -> None:
    """Test that the factory returns the handler matching the requested engine."""
    assert isinstance(_get_job_handler(JobEngines.SLURM), SlurmSSHJobHandler)
    assert isinstance(_get_job_handler(JobEngines.FILE_BASED), FileBasedJobHandler)
    assert isinstance(_get_job_handler(JobEngines.GENERIC), GenericJobHandler)


def test_get_job_handler_docker_without_optional_dependency() -> None:
    """Test that a missing optional `docker` dependency points to the requirements file."""
    # a None entry in sys.modules makes the import of that module raise an ImportError
    with (
        patch.dict(sys.modules, {"docker": None, "jobs.docker_job_handler": None}),
        pytest.raises(AirflowFailException, match="requirements_docker_job_engine"),
    ):
        _get_job_handler(JobEngines.DOCKER)


@yaml_locations(slurm="/path/to/slurm_base_path")
@patch("jobs.job_handler.ssh_execute")
def test_start_job_returns_valid_job_id(mock_ssh_execute: MagicMock) -> None:
    """Test that start_job returns a valid job ID."""
    mock_ssh_execute.return_value = "12345"

    environment = {
        "ENV_VAR": "value",
        "_SLURM_CPUS_PER_TASK": 8,
        "_SLURM_MEM": "62G",
        "_SLURM_TIME": "02:00:00",
    }

    # when
    job_id = SlurmSSHJobHandler().start_job("submit_job.sh", environment, "2024_07")
    assert job_id == "12345"
    expected_command = (
        'export ENV_VAR="value"\n'
        "mkdir -p /path/to/slurm_base_path/jobs/2024_07\n"
        "cd /path/to/slurm_base_path/jobs/2024_07\n"
        "cat /path/to/slurm_base_path/submit_job.sh\n"
        "JID=$(sbatch --cpus-per-task=8 --mem=62G --time=02:00:00 /path/to/slurm_base_path/submit_job.sh)\n"
        "echo ${JID##* }"
    )
    mock_ssh_execute.assert_called_once_with(expected_command)


@yaml_locations(slurm="/path/to/slurm_base_path")
@patch("jobs.job_handler.ssh_execute")
def test_start_job_handles_invalid_job_id(mock_ssh_execute: MagicMock) -> None:
    """Test that start_job raises an exception when the job ID is invalid."""
    mock_ssh_execute.return_value = "invalid_id"

    environment = {
        "ENV_VAR": "value",
        "_SLURM_CPUS_PER_TASK": 8,
        "_SLURM_MEM": "62G",
        "_SLURM_TIME": "02:00:00",
    }

    with pytest.raises(AirflowFailException, match="Job submission failed."):
        SlurmSSHJobHandler().start_job("submit_job.sh", environment, "2024_07")


@yaml_locations(slurm="/path/to/slurm_base_path")
@patch("jobs.job_handler.ssh_execute")
def test_get_job_status_returns_correctly(mock_ssh_execute: MagicMock) -> None:
    """Test that get_job_status returns the correct status."""
    mock_ssh_execute.return_value = "COMPLETED"

    job_status = SlurmSSHJobHandler().get_job_status("12345")
    assert job_status == "COMPLETED"
    expected_command = (
        "JOB_INFO=$(scontrol show job 12345 2>/dev/null)\n"
        "if [ $? -eq 0 ]; then\n"
        'ST=$(echo "$JOB_INFO" | grep JobState | '
        "awk -F 'JobState=' '{print $2}' | awk -F ' ' '{print $1}')\n"
        "else\n"
        "ST=$(sacct -j 12345 -o State 2>/dev/null | awk 'FNR == 3 {print $1}')\n"
        "fi\n"
        'echo "${ST:-UNKNOWN}"'
    )
    mock_ssh_execute.assert_called_once_with(expected_command)


@yaml_locations(slurm="/path/to/slurm_base_path")
@patch("jobs.job_handler.ssh_execute")
def test_get_job_result_returns_correct_job_status_and_time_elapsed(
    mock_ssh_execute: MagicMock,
) -> None:
    """Test that get_job_result returns the correct job status and time elapsed."""
    mock_ssh_execute.return_value = "00:08:42\nCOMPLETED"

    # when
    job_status, time_elapsed = SlurmSSHJobHandler().get_job_result("12345")
    assert job_status == "COMPLETED"
    assert time_elapsed == 8 * 60 + 42
    expected_command = (
        "SACCT_OUT=$(sacct -l --parsable2 -j 12345 2>/dev/null)\n"
        "if [ $? -eq 0 ]; then\n"
        "echo \"$SACCT_OUT\" | awk -F'|' 'NR==1{for(i=1;i<=NF;i++)if($i==\"Elapsed\")c=i}END{print $c}'\n"
        'echo "$SACCT_OUT"\n'
        "else\n"
        'echo "00:00:00"\n'
        "fi\n"
        "cat /path/to/slurm_base_path/jobs/*/slurm-12345.out\n"
        "JOB_INFO=$(scontrol show job 12345 2>/dev/null)\n"
        "if [ $? -eq 0 ]; then\n"
        'ST=$(echo "$JOB_INFO" | grep JobState | '
        "awk -F 'JobState=' '{print $2}' | awk -F ' ' '{print $1}')\n"
        "else\n"
        "ST=$(sacct -j 12345 -o State 2>/dev/null | awk 'FNR == 3 {print $1}')\n"
        "fi\n"
        'echo "${ST:-UNKNOWN}"'
    )
    mock_ssh_execute.assert_called_once_with(expected_command)


@pytest.mark.parametrize(
    ("engine", "expected_type"),
    [
        ("slurm_no_sacct", SlurmNoSacctSSHJobHandler),
        ("slurm", SlurmSSHJobHandler),
    ],
)
@yaml_locations(slurm="/path/to/slurm_base_path")
def test_get_job_handler_returns_correct_handler(
    engine: str | None, expected_type: type
) -> None:
    """Test that the factory returns the correct handler for the given engine."""
    handler = _get_job_handler(engine)

    assert type(handler) is expected_type


@yaml_locations(slurm="/path/to/slurm_base_path")
@patch("jobs.job_handler.ssh_execute")
def test_no_sacct_get_job_status_returns_correctly(mock_ssh_execute: MagicMock) -> None:
    """Test that get_job_status uses scontrol instead of sacct."""
    mock_ssh_execute.return_value = "RUNNING"

    job_status = SlurmNoSacctSSHJobHandler().get_job_status("12345")
    assert job_status == "RUNNING"
    expected_command = (
        "ST=$(scontrol show job 12345 2>/dev/null | grep JobState | "
        "awk -F 'JobState=' '{print $2}' | awk -F ' ' '{print $1}')\n"
        'echo "${ST:-UNKNOWN}"'
    )
    mock_ssh_execute.assert_called_once_with(expected_command)


@yaml_locations(slurm="/path/to/slurm_base_path")
@patch("jobs.job_handler.ssh_execute")
def test_no_sacct_get_job_result_returns_correct_job_status_and_time_elapsed(
    mock_ssh_execute: MagicMock,
) -> None:
    """Test that get_job_result works without sacct and reports zero time elapsed."""
    mock_ssh_execute.return_value = "00:00:00\nRUNNING"

    # when
    job_status, time_elapsed = SlurmNoSacctSSHJobHandler().get_job_result("12345")
    assert job_status == "RUNNING"
    assert time_elapsed == 0
    expected_command = (
        "TIME_ELAPSED=00:00:00\n"
        "echo $TIME_ELAPSED\n"
        "cat /path/to/slurm_base_path/jobs/*/slurm-12345.out\n"
        "ST=$(scontrol show job 12345 2>/dev/null | grep JobState | "
        "awk -F 'JobState=' '{print $2}' | awk -F ' ' '{print $1}')\n"
        'echo "${ST:-UNKNOWN}"'
    )
    mock_ssh_execute.assert_called_once_with(expected_command)
