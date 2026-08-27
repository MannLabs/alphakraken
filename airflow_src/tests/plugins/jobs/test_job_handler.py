"""Tests for the job_handler module."""

import importlib.util
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowFailException
from jobs._experimental.file_based_job_handler import FileBasedJobHandler
from jobs.job_handler import _get_job_handler
from jobs.slurm_ssh_job_handler import SlurmSSHJobHandler

from airflow_src.tests.helpers import yaml_locations
from shared.keys import JobEngines

# `docker` is an optional dependency, cf. requirements_docker_job_engine.txt
HAS_DOCKER = importlib.util.find_spec("docker") is not None

SLURM_BASE_DIR = Path("/path/to/slurm_base_path")
HOST_MOUNTS_PATH = Path("/host/mounts")


@yaml_locations(slurm=str(SLURM_BASE_DIR))
def test_get_job_handler_routes_engine_to_handler() -> None:
    """Test that the factory returns the handler matching the requested engine."""
    assert isinstance(_get_job_handler(JobEngines.SLURM), SlurmSSHJobHandler)
    assert isinstance(_get_job_handler(JobEngines.FILE_BASED), FileBasedJobHandler)


@yaml_locations(slurm=str(SLURM_BASE_DIR))
def test_get_job_handler_injects_slurm_base_dir() -> None:
    """Test that the factory reads the base dir and hands it to the Slurm handler."""
    handler = _get_job_handler(JobEngines.SLURM)

    assert handler._cluster_base_dir == SLURM_BASE_DIR


@pytest.mark.skipif(not HAS_DOCKER, reason="`docker` not installed")
@patch("jobs.job_handler.get_host_mounts_path")
@patch("jobs.docker_job_handler.docker.from_env")
def test_get_job_handler_injects_host_mounts_path(
    mock_from_env: MagicMock,  # noqa: ARG001
    mock_get_host_mounts_path: MagicMock,
) -> None:
    """Test that the factory reads the mounts path and hands it to the docker handler."""
    mock_get_host_mounts_path.return_value = HOST_MOUNTS_PATH

    handler = _get_job_handler(JobEngines.DOCKER)

    assert handler._host_mounts_path == HOST_MOUNTS_PATH


def test_get_job_handler_docker_without_optional_dependency() -> None:
    """Test that a missing optional `docker` dependency points to the requirements file."""
    # a None entry in sys.modules makes the import of that module raise an ImportError
    with (
        patch.dict(sys.modules, {"docker": None, "jobs.docker_job_handler": None}),
        pytest.raises(AirflowFailException, match="requirements_docker_job_engine"),
    ):
        _get_job_handler(JobEngines.DOCKER)
