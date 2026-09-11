"""Module containing the commands to interact with job clusters.

This module provides the abstract interface for job execution on different engines
and the factory dispatching to the concrete implementations.
"""

import abc
import logging

from airflow.exceptions import AirflowFailException
from common.quanting_env import QuantingEnv

from shared.keys import JobEngines
from shared.yamlsettings import YamlKeys, get_host_mounts_path, get_path


def _get_job_handler(engine: str) -> "JobHandler":
    """Factory function to get the appropriate job handler for the given engine."""
    if engine == JobEngines.SLURM:
        from jobs.slurm_ssh_job_handler import SlurmSSHJobHandler

        logging.info("Using SlurmSSHJobHandler")
        return SlurmSSHJobHandler(get_path(YamlKeys.Locations.SLURM))

    if engine == JobEngines.DOCKER:
        try:
            from jobs.docker_job_handler import DockerJobHandler
        except ImportError as e:
            raise AirflowFailException(
                f"The '{JobEngines.DOCKER}' job engine requires the optional requirements in "
                f"airflow_src/requirements_docker_job_engine.txt to be installed."
            ) from e

        logging.info("Using DockerJobHandler")
        return DockerJobHandler(get_host_mounts_path())

    if engine == JobEngines.FILE_BASED:
        from jobs._experimental.file_based_job_handler import FileBasedJobHandler

        logging.info("Using FileBasedJobHandler")
        return FileBasedJobHandler()

    raise ValueError(f"Unsupported job engine: {engine}")


class JobHandler(abc.ABC):
    """Abstract base class for job handling."""

    @abc.abstractmethod
    def start_job(self, quanting_env: QuantingEnv) -> str:
        """Start a job and return the job ID.

        Args:
            quanting_env: Environment of the job to submit

        Returns:
            Job ID as a string

        """

    @abc.abstractmethod
    def get_job_status(self, job_id: str) -> str:
        """Get the status of a job."""

    @abc.abstractmethod
    def get_job_result(self, job_id: str) -> tuple[str, int]:
        """Get the job status and execution time from a running or completed job.

        Args:
            job_id: job ID to query

        Returns:
            Tuple of (job_status, time_elapsed_seconds)

        """


def start_job(
    quanting_env: QuantingEnv,
    engine: str,
) -> str:
    """Start a job using the given job engine.

    Delegates to JobHandler.start_job(), see docs there.
    """
    handler = _get_job_handler(engine)
    return handler.start_job(quanting_env)


def get_job_status(job_id: str, engine: str) -> str:
    """Get the job status using the given job engine.

    Delegates to JobHandler.get_job_status(), see docs there.
    """
    handler = _get_job_handler(engine)
    return handler.get_job_status(job_id)


def get_job_result(job_id: str, engine: str) -> tuple[str, int]:
    """Get the job status and time elapsed using the given job engine."""
    handler = _get_job_handler(engine)
    return handler.get_job_result(job_id)
