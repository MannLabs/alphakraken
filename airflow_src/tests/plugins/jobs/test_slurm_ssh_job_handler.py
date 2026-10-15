"""Tests for the slurm_ssh_job_handler module."""

from collections.abc import Callable
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import pytz
from airflow.sdk.exceptions import AirflowFailException
from common.quanting_env import QuantingEnv
from jobs.slurm_ssh_job_handler import SlurmSSHJobHandler

SLURM_BASE_DIR = Path("/path/to/slurm_base_path")
SSH_PREFIX = "some_cluster_ssh"

EXPECTED_EXPORTS = (
    'export RAW_FILE_PATH="/pool/backup/instrument1/1970_01/test_file.raw"\n'
    'export SETTINGS_PATH="/pool/settings/test_settings"\n'
    'export OUTPUT_PATH="/pool/output/PID1/out_test_file.raw/test_settings_v1"\n'
    'export RELATIVE_OUTPUT_PATH="PID1/out_test_file.raw/test_settings_v1"\n'
    'export SPECLIB_FILE_NAME="some_speclib_file_name"\n'
    'export FASTA_FILE_NAME="some_fasta_file_name"\n'
    'export CONFIG_FILE_NAME="some_config_file_name"\n'
    'export SOFTWARE="some_software"\n'
    'export SOFTWARE_TYPE="alphadia"\n'
    'export METRICS_TYPE="alphadia"\n'
    'export CUSTOM_COMMAND=""\n'
    'export NUM_THREADS="8"\n'
    'export RAW_FILE_ID="test_file.raw"\n'
    'export PROJECT_ID="PID1"\n'
    'export SETTINGS_NAME="test_settings"\n'
    'export SETTINGS_VERSION="1"'
)


@patch("jobs.slurm_ssh_job_handler.ssh_execute")
def test_start_job_returns_valid_job_id(
    mock_ssh_execute: MagicMock,
    make_quanting_env: Callable[..., QuantingEnv],
) -> None:
    """Test that start_job returns a valid job ID."""
    mock_ssh_execute.return_value = "12345"

    quanting_env = make_quanting_env()

    # when
    job_id = SlurmSSHJobHandler(SLURM_BASE_DIR, SSH_PREFIX).start_job(quanting_env)
    assert job_id == "12345"
    expected_command = (
        f"{EXPECTED_EXPORTS}\n"
        "mkdir -p /pool/output/PID1/out_test_file.raw/test_settings_v1\n"
        "cd /pool/output/PID1/out_test_file.raw/test_settings_v1\n"
        "cat /path/to/slurm_base_path/submit_slurm_job.sh\n"
        "JID=$(sbatch --cpus-per-task=8 --mem=62G --time=02:00:00 /path/to/slurm_base_path/submit_slurm_job.sh)\n"
        "echo ${JID##* }"
    )
    mock_ssh_execute.assert_called_once_with(expected_command, SSH_PREFIX)


@patch("jobs.slurm_ssh_job_handler.ssh_execute")
def test_start_job_handles_invalid_job_id(
    mock_ssh_execute: MagicMock,
    make_quanting_env: Callable[..., QuantingEnv],
) -> None:
    """Test that start_job raises an exception when the job ID is invalid."""
    mock_ssh_execute.return_value = "invalid_id"

    with pytest.raises(AirflowFailException, match="Job submission failed."):
        SlurmSSHJobHandler(SLURM_BASE_DIR, SSH_PREFIX).start_job(make_quanting_env())


@patch("jobs.slurm_ssh_job_handler.get_slurm_job", return_value=None)
@patch("jobs.slurm_ssh_job_handler.ssh_execute")
def test_get_job_status_returns_correctly(
    mock_ssh_execute: MagicMock,
    mock_get_slurm_job: MagicMock,  # noqa: ARG001
) -> None:
    """Test that get_job_status returns the correct status."""
    mock_ssh_execute.return_value = "COMPLETED"

    job_status = SlurmSSHJobHandler(SLURM_BASE_DIR, SSH_PREFIX).get_job_status("12345")
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
    mock_ssh_execute.assert_called_once_with(expected_command, SSH_PREFIX)


def _db_job(state: str, age_s: int) -> MagicMock:
    """Create a mock DB job of the given age, with a naive UTC timestamp as returned by the DB."""
    return MagicMock(
        state=state,
        updated_at_=datetime.now(tz=pytz.utc).replace(tzinfo=None)
        - timedelta(seconds=age_s),
    )


@pytest.mark.parametrize(
    ("state", "age_s"),
    [
        ("RUNNING", 1),
        ("LEFT_QUEUE", 10_000),  # final, so age does not matter
    ],
)
@patch("jobs.slurm_ssh_job_handler.get_slurm_job")
@patch("jobs.slurm_ssh_job_handler.ssh_execute")
def test_get_job_status_returns_status_from_db(
    mock_ssh_execute: MagicMock,
    mock_get_slurm_job: MagicMock,
    state: str,
    age_s: int,
) -> None:
    """Test that get_job_status takes a fresh or final status from the DB without SSH."""
    mock_get_slurm_job.return_value = _db_job(state, age_s)

    # when
    job_status = SlurmSSHJobHandler(SLURM_BASE_DIR, SSH_PREFIX).get_job_status("12345")

    # then
    assert job_status == state
    mock_get_slurm_job.assert_called_once_with(SSH_PREFIX, "12345")
    mock_ssh_execute.assert_not_called()


@pytest.mark.parametrize(
    "db_job",
    [
        None,  # job not yet polled
        _db_job("RUNNING", 10_000),  # stale
    ],
)
@patch("jobs.slurm_ssh_job_handler.get_slurm_job")
@patch("jobs.slurm_ssh_job_handler.ssh_execute")
def test_get_job_status_falls_back_to_ssh(
    mock_ssh_execute: MagicMock,
    mock_get_slurm_job: MagicMock,
    db_job: MagicMock | None,
) -> None:
    """Test that get_job_status queries the cluster if the job is not in the DB or its status is stale."""
    mock_get_slurm_job.return_value = db_job
    mock_ssh_execute.return_value = "COMPLETED"

    # when
    job_status = SlurmSSHJobHandler(SLURM_BASE_DIR, SSH_PREFIX).get_job_status("12345")

    # then
    assert job_status == "COMPLETED"
    mock_ssh_execute.assert_called_once()


@patch("jobs.slurm_ssh_job_handler.ssh_execute")
def test_get_job_result_returns_correct_job_status_and_time_elapsed(
    mock_ssh_execute: MagicMock,
) -> None:
    """Test that get_job_result returns the correct job status and time elapsed."""
    mock_ssh_execute.return_value = "00:08:42\nCOMPLETED"

    # when
    job_status, time_elapsed = SlurmSSHJobHandler(
        SLURM_BASE_DIR, SSH_PREFIX
    ).get_job_result("12345")
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
        # "cat /path/to/slurm_base_path/jobs/*/slurm-12345.out\n"
        "JOB_INFO=$(scontrol show job 12345 2>/dev/null)\n"
        "if [ $? -eq 0 ]; then\n"
        'ST=$(echo "$JOB_INFO" | grep JobState | '
        "awk -F 'JobState=' '{print $2}' | awk -F ' ' '{print $1}')\n"
        "else\n"
        "ST=$(sacct -j 12345 -o State 2>/dev/null | awk 'FNR == 3 {print $1}')\n"
        "fi\n"
        'echo "${ST:-UNKNOWN}"'
    )
    mock_ssh_execute.assert_called_once_with(expected_command, SSH_PREFIX)


@patch("jobs.slurm_ssh_job_handler.ssh_execute")
def test_get_job_result_returns_zero_time_elapsed_on_unparseable_timestamp(
    mock_ssh_execute: MagicMock,
) -> None:
    """Test that get_job_result reports zero time elapsed if sacct gave no usable output."""
    mock_ssh_execute.return_value = "some slurm log line\nCOMPLETED"

    # when
    job_status, time_elapsed = SlurmSSHJobHandler(
        SLURM_BASE_DIR, SSH_PREFIX
    ).get_job_result("12345")

    assert job_status == "COMPLETED"
    assert time_elapsed == 0
