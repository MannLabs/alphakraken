"""Tests for the job_handler module."""

import os
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowFailException
from jobs.job_handler import SlurmSSHJobHandler


@patch.dict(os.environ, {"SLURM_BASE_PATH": "/path/to/slurm_base_path"})
@patch("jobs.job_handler.ssh_execute")
def test_start_job_returns_valid_job_id(mock_ssh_execute: MagicMock) -> None:
    """Test that start_job returns a valid job ID."""
    mock_ssh_execute.return_value = "12345"
    # when
    job_id = SlurmSSHJobHandler().start_job({"ENV_VAR": "value"}, "2024_07")
    assert job_id == "12345"
    expected_command = (
        "export ENV_VAR=value\n"
        "mkdir -p /path/to/slurm_base_path/jobs/2024_07\n"
        "cd /path/to/slurm_base_path/jobs/2024_07\n"
        "cat /path/to/slurm_base_path/submit_job.sh\n"
        "JID=$(sbatch /path/to/slurm_base_path/submit_job.sh)\n"
        "echo ${JID##* }"
    )
    mock_ssh_execute.assert_called_once_with(expected_command)


@patch.dict(os.environ, {"SLURM_BASE_PATH": "/path/to/slurm_base_path"})
@patch("jobs.job_handler.ssh_execute")
def test_start_job_handles_invalid_job_id(mock_ssh_execute: MagicMock) -> None:
    """Test that start_job raises an exception when the job ID is invalid."""
    mock_ssh_execute.return_value = "invalid_id"

    with pytest.raises(AirflowFailException, match="Job submission failed."):
        SlurmSSHJobHandler().start_job({"ENV_VAR": "value"}, "2024_07")


@patch.dict(os.environ, {"SLURM_BASE_PATH": "/path/to/slurm_base_path"})
@patch("jobs.job_handler.ssh_execute")
def test_get_job_status_returns_correctly(mock_ssh_execute: MagicMock) -> None:
    """Test that get_job_status returns the correct status."""
    mock_ssh_execute.return_value = "COMPLETED"

    job_status = SlurmSSHJobHandler().get_job_status("12345")
    assert job_status == "COMPLETED"
    expected_command = (
        "ST=$(sacct -j 12345 -o State | awk 'FNR == 3 {print $1}')\necho $ST"
    )
    mock_ssh_execute.assert_called_once_with(expected_command)


@patch.dict(os.environ, {"SLURM_BASE_PATH": "/path/to/slurm_base_path"})
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
        "TIME_ELAPSED=$(sacct --format=Elapsed -j 12345 | tail -n 1); echo $TIME_ELAPSED\n"
        "sacct -l -j 12345\n"
        "cat /path/to/slurm_base_path/jobs/*/slurm-12345.out\n"
        "ST=$(sacct -j 12345 -o State | awk 'FNR == 3 {print $1}')\n"
        "echo $ST"
    )
    mock_ssh_execute.assert_called_once_with(expected_command)
