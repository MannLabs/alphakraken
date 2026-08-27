"""Tests for the slurm_ssh_job_handler module."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowFailException
from jobs.slurm_ssh_job_handler import SlurmSSHJobHandler


@patch("jobs.slurm_ssh_job_handler.get_path")
@patch("jobs.slurm_ssh_job_handler.ssh_execute")
def test_start_job_returns_valid_job_id(
    mock_ssh_execute: MagicMock, mock_get_path: MagicMock
) -> None:
    """Test that start_job returns a valid job ID."""
    mock_get_path.return_value = Path("/path/to/slurm_base_path")
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


@patch("jobs.slurm_ssh_job_handler.get_path")
@patch("jobs.slurm_ssh_job_handler.ssh_execute")
def test_start_job_handles_invalid_job_id(
    mock_ssh_execute: MagicMock, mock_get_path: MagicMock
) -> None:
    """Test that start_job raises an exception when the job ID is invalid."""
    mock_get_path.return_value = Path("/path/to/slurm_base_path")
    mock_ssh_execute.return_value = "invalid_id"

    environment = {
        "ENV_VAR": "value",
        "_SLURM_CPUS_PER_TASK": 8,
        "_SLURM_MEM": "62G",
        "_SLURM_TIME": "02:00:00",
    }

    with pytest.raises(AirflowFailException, match="Job submission failed."):
        SlurmSSHJobHandler().start_job("submit_job.sh", environment, "2024_07")


@patch("jobs.slurm_ssh_job_handler.get_path")
@patch("jobs.slurm_ssh_job_handler.ssh_execute")
def test_get_job_status_returns_correctly(
    mock_ssh_execute: MagicMock, mock_get_path: MagicMock
) -> None:
    """Test that get_job_status returns the correct status."""
    mock_get_path.return_value = Path("/path/to/slurm_base_path")
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


@patch("jobs.slurm_ssh_job_handler.get_path")
@patch("jobs.slurm_ssh_job_handler.ssh_execute")
def test_get_job_result_returns_correct_job_status_and_time_elapsed(
    mock_ssh_execute: MagicMock, mock_get_path: MagicMock
) -> None:
    """Test that get_job_result returns the correct job status and time elapsed."""
    mock_get_path.return_value = Path("/path/to/slurm_base_path")
    mock_ssh_execute.return_value = "00:08:42\nCOMPLETED"

    # when
    job_status, time_elapsed = SlurmSSHJobHandler().get_job_result("12345")
    assert job_status == "COMPLETED"
    assert time_elapsed == 8 * 60 + 42
    expected_command = (
        "SACCT_OUT=$(sacct -l --parsable2 -j 12345 2>/dev/null)\n"
        "if [ $? -eq 0 ]; then\n"
        'echo "$SACCT_OUT" | awk -F\'|\' \'NR==1{for(i=1;i<=NF;i++)if($i=="Elapsed")c=i}END{print (c && NR>1) ? $c : "00:00:00"}\'\n'
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


@patch("jobs.slurm_ssh_job_handler.get_path")
@patch("jobs.slurm_ssh_job_handler.ssh_execute")
def test_get_job_result_returns_zero_time_elapsed_on_unparseable_timestamp(
    mock_ssh_execute: MagicMock, mock_get_path: MagicMock
) -> None:
    """Test that get_job_result reports zero time elapsed if sacct gave no usable output."""
    mock_get_path.return_value = Path("/path/to/slurm_base_path")
    mock_ssh_execute.return_value = "some slurm log line\nCOMPLETED"

    # when
    job_status, time_elapsed = SlurmSSHJobHandler().get_job_result("12345")

    assert job_status == "COMPLETED"
    assert time_elapsed == 0
