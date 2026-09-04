"""Tests for the job_handler module."""

import importlib.util
import sys
from collections.abc import Callable
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowFailException
from common.quanting_env import QuantingEnv
from jobs._experimental.file_based_job_handler import FileBasedJobHandler
from jobs.job_handler import _get_job_handler, start_job
from jobs.slurm_ssh_job_handler import SlurmSSHJobHandler

from airflow_src.tests.helpers import yaml_locations
from shared.keys import JobEngines
from shared.path_views import DOCKER_HOST_VIEW
from shared.runners import Runner, get_runner

# `docker` is an optional dependency, cf. requirements_docker_job_engine.txt
HAS_DOCKER = importlib.util.find_spec("docker") is not None

SLURM_BASE_DIR = Path("/path/to/slurm_base_path")


@yaml_locations(slurm=str(SLURM_BASE_DIR))
def test_get_job_handler_routes_engine_to_handler() -> None:
    """Test that the factory returns the handler matching the requested engine."""
    assert isinstance(
        _get_job_handler(get_runner(JobEngines.SLURM)), SlurmSSHJobHandler
    )
    assert isinstance(
        _get_job_handler(get_runner(JobEngines.FILE_BASED)), FileBasedJobHandler
    )


@yaml_locations(slurm=str(SLURM_BASE_DIR))
def test_get_job_handler_injects_slurm_base_dir() -> None:
    """Test that the factory reads the base dir and hands it to the Slurm handler."""
    handler = _get_job_handler(get_runner(JobEngines.SLURM))

    assert handler._cluster_base_dir == SLURM_BASE_DIR


@pytest.mark.skipif(not HAS_DOCKER, reason="`docker` not installed")
@patch("jobs.docker_job_handler.docker.from_env")
def test_get_job_handler_injects_docker_host_view(
    mock_from_env: MagicMock,  # noqa: ARG001
) -> None:
    """Test that the factory hands the docker host view to the docker handler."""
    handler = _get_job_handler(get_runner(JobEngines.DOCKER))

    assert handler._docker_host_view is DOCKER_HOST_VIEW


@pytest.mark.skipif(not HAS_DOCKER, reason="`docker` not installed")
@patch("jobs.docker_job_handler.docker.from_env")
def test_get_job_handler_docker_without_mounts_env(
    mock_from_env: MagicMock,  # noqa: ARG001
) -> None:
    """Test that a docker host view without locations points to the missing environment variable."""
    with (
        patch.object(DOCKER_HOST_VIEW, "_locations", {}),
        pytest.raises(AirflowFailException, match="MOUNTS_PATH"),
    ):
        _get_job_handler(get_runner(JobEngines.DOCKER))


def test_get_job_handler_docker_without_optional_dependency() -> None:
    """Test that a missing optional `docker` dependency points to the requirements file."""
    # a None entry in sys.modules makes the import of that module raise an ImportError
    with (
        patch.dict(sys.modules, {"docker": None, "jobs.docker_job_handler": None}),
        pytest.raises(AirflowFailException, match="requirements_docker_job_engine"),
    ):
        _get_job_handler(get_runner(JobEngines.DOCKER))


def test_get_job_handler_rejects_unknown_engine() -> None:
    """Test that a runner with an engine the factory has no branch for is rejected."""
    runner = Runner(
        name="k8s",
        engine="kubernetes",
        os="linux",
        view=MagicMock(),
        ssh_connection_id_prefix=None,
    )

    with pytest.raises(ValueError, match="kubernetes"):
        _get_job_handler(runner)


def test_start_job_rejects_undeclared_runner(
    make_quanting_env: Callable[..., QuantingEnv],
) -> None:
    """Test that a runner name not in the yaml fails listing the declared runners."""
    with pytest.raises(KeyError, match=r"'nope'.*\['slurm', 'docker', 'file_based'\]"):
        start_job(make_quanting_env(), runner_name="nope")
