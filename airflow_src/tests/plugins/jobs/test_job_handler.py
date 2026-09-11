"""Tests for the job_handler module."""

import importlib.util
import sys
from collections.abc import Callable
from pathlib import PurePosixPath
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowFailException
from common.quanting_env import QuantingEnv
from jobs._experimental.file_based_job_handler import FileBasedJobHandler
from jobs.job_handler import _get_job_handler, start_job
from jobs.simple_ssh_job_handler import _OS_TO_DIALECT, SimpleSSHJobHandler
from jobs.slurm_ssh_job_handler import SlurmSSHJobHandler

from shared.keys import JobEngines
from shared.path_views import DOCKER_HOST_VIEW, Locations, View
from shared.runners import OperatingSystems, Runner, get_runner

# `docker` is an optional dependency, cf. requirements_docker_job_engine.txt
HAS_DOCKER = importlib.util.find_spec("docker") is not None

OUTPUT_DIR = PurePosixPath("/path/to/output")
SOFTWARE_DIR = PurePosixPath("/path/to/software")
SSH_PREFIX = "some_cluster_ssh"


def _runner(engine: str, ssh_connection_id_prefix: str | None = SSH_PREFIX) -> Runner:
    """Get a linux runner of the given engine whose view reaches the software location."""
    return Runner(
        name=f"{engine}_runner",
        engine=engine,
        os=OperatingSystems.LINUX,
        view=View(
            "test",
            {Locations.OUTPUT: str(OUTPUT_DIR), Locations.SOFTWARE: str(SOFTWARE_DIR)},
            PurePosixPath,
        ),
        ssh_connection_id_prefix=ssh_connection_id_prefix,
    )


def test_get_job_handler_routes_engine_to_handler() -> None:
    """Test that the factory returns the handler matching the engine of the runner."""
    assert isinstance(_get_job_handler(_runner(JobEngines.SLURM)), SlurmSSHJobHandler)
    assert isinstance(
        _get_job_handler(_runner(JobEngines.FILE_BASED)), FileBasedJobHandler
    )
    assert isinstance(
        _get_job_handler(_runner(JobEngines.SIMPLE_SSH)), SimpleSSHJobHandler
    )


def test_get_job_handler_injects_job_script_dir_and_ssh_prefix() -> None:
    """Test that the Slurm handler gets the runner's software location and SSH prefix."""
    handler = _get_job_handler(_runner(JobEngines.SLURM))

    assert handler._job_script_dir == SOFTWARE_DIR
    assert handler._ssh_connection_id_prefix == SSH_PREFIX


def test_get_job_handler_injects_output_dir_os_and_ssh_prefix() -> None:
    """Test that the simple_ssh handler gets the runner's output location, os and SSH prefix."""
    handler = _get_job_handler(_runner(JobEngines.SIMPLE_SSH))

    assert handler._output_dir == OUTPUT_DIR
    assert handler._dialect is _OS_TO_DIALECT[OperatingSystems.LINUX]
    assert handler._ssh_connection_id_prefix == SSH_PREFIX


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
