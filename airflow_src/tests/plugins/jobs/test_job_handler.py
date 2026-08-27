"""Tests for the job_handler module."""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowFailException
from jobs._experimental.file_based_job_handler import FileBasedJobHandler
from jobs.job_handler import _get_job_handler
from jobs.slurm_ssh_job_handler import SlurmSSHJobHandler

from shared.keys import JobEngines


@patch("jobs.slurm_ssh_job_handler.get_path")
def test_get_job_handler_routes_engine_to_handler(mock_get_path: MagicMock) -> None:
    """Test that the factory returns the handler matching the requested engine."""
    mock_get_path.return_value = Path("/path/to/slurm_base_path")

    assert isinstance(_get_job_handler(JobEngines.SLURM), SlurmSSHJobHandler)
    assert isinstance(_get_job_handler(JobEngines.FILE_BASED), FileBasedJobHandler)


def test_get_job_handler_docker_without_optional_dependency() -> None:
    """Test that a missing optional `docker` dependency points to the requirements file."""
    # a None entry in sys.modules makes the import of that module raise an ImportError
    with (
        patch.dict(sys.modules, {"docker": None, "jobs.docker_job_handler": None}),
        pytest.raises(AirflowFailException, match="requirements_docker_job_engine"),
    ):
        _get_job_handler(JobEngines.DOCKER)
