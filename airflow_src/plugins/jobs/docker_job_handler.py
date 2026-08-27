"""Implementation of a job handler that runs jobs in Docker containers on the AlphaKraken host.

This is the execution engine for standalone deployments that have no external compute
resources: instead of submitting to Slurm, the Airflow worker starts one sibling container
per job via the Docker socket.

The image is taken from the `software` field of the settings, and the resolved configuration
parameters are passed to it as the container command, so an image behaves like a custom command
that happens to run in a container. The raw file and the output folder are bound into the
container at the very paths the placeholders resolved to, which makes the same `config_params`
work for both this engine and Slurm.

Notes:
    - requires the optional requirements in `requirements_docker_job_engine.txt`.
    - the image must already be present on the host, it is never pulled, cf. `_get_image`.
    - requires the bind mount of the docker socket in docker-compose.yaml (cf. `group_add`).
    - requires key 'locations.general.mounts_path' in alphakraken.{env}.yaml to point to the
      mounts folder as seen by the docker host.
    - `_SLURM_TIME` is not honored: docker has no wall clock limit.

"""

import logging
import os
import re
import shlex
from datetime import datetime
from pathlib import Path

import docker
from airflow.exceptions import AirflowFailException
from common.keys import JobStates
from common.quanting_env import QuantingEnv
from docker.errors import ImageNotFound, NotFound
from docker.models.containers import Container
from jobs.job_handler import JobHandler

from shared.keys import InternalPaths

CONTAINER_NAME_PREFIX = "kraken"
# docker accepts only [a-zA-Z0-9][a-zA-Z0-9_.-]* as container name, but raw file names may
# contain e.g. '+', cf. `_ALLOWED_RAW_FILE_NAME_CHARACTERS`
_FORBIDDEN_CONTAINER_NAME_CHARACTERS_PATTERN = r"[^a-zA-Z0-9_.-]"

JOB_LABEL = "alphakraken.job"
OUTPUT_PATH_LABEL = "alphakraken.internal_output_path"

LOG_FILE_NAME = "log.txt"

# docker reports this instead of a real timestamp for a container that has not finished yet
UNSET_TIMESTAMP = "0001-01-01"

# docker container states, cf. https://docs.docker.com/reference/cli/docker/container/ls/#status
_DOCKER_STATE_TO_JOB_STATE = {
    "created": JobStates.PENDING,
    "running": JobStates.RUNNING,
    "restarting": JobStates.RUNNING,
    "paused": JobStates.RUNNING,
    "removing": JobStates.COMPLETING,
}

NANO_CPUS_PER_CPU = 1_000_000_000


class DockerJobHandler(JobHandler):
    """Implementation of JobHandler that runs jobs in Docker containers on the AlphaKraken host."""

    def __init__(self, host_mounts_path: Path):
        """Initialize the docker job handler.

        Args:
            host_mounts_path: Path of the mounts folder as seen by the docker host
                (not by the containers)

        """
        super().__init__()
        self._client = docker.from_env()
        self._host_mounts_path = host_mounts_path

    def start_job(
        self,
        job_script_name: str,
        quanting_env: QuantingEnv,
        year_month_folder: str,
    ) -> str:
        """Start a job by running a container on the AlphaKraken host.

        Args:
            job_script_name: Name of the job script (ignored for this handler)
            quanting_env: Environment of the quanting job
            year_month_folder: Folder for job outputs (ignored for this handler)

        Returns:
            Job ID (in the case of this handler, the short container id)

        """
        del job_script_name  # unused
        del year_month_folder  # unused

        image = self._get_image(quanting_env.software)
        # None makes docker use the command defined in the image
        command = shlex.split(quanting_env.config_params) or None

        internal_raw_file_path = Path(quanting_env.internal_raw_file_path)
        internal_output_path = Path(quanting_env.internal_output_path)
        for path in (internal_raw_file_path, internal_output_path):
            if not path.exists():
                raise AirflowFailException(f"Path {path} does not exist in the worker.")

        container_name = _to_container_name(
            f"{CONTAINER_NAME_PREFIX}-{quanting_env.software_type}-"
            f"{quanting_env.raw_file_id}"
        )
        self._remove_container(container_name)

        # bind at the paths the placeholders in the config params resolved to, so that the same
        # config params work for this engine and for Slurm
        volumes = {
            str(self._to_host_path(internal_raw_file_path)): {
                "bind": quanting_env.raw_file_path,
                "mode": "ro",
            },
            str(self._to_host_path(internal_output_path)): {
                "bind": quanting_env.output_path,
                "mode": "rw",
            },
        }

        logging.info(
            f"Running image {image} as {container_name} with {command=} and {volumes=}"
        )

        container = self._client.containers.run(
            image,
            command,
            detach=True,
            name=container_name,
            volumes=volumes,
            # the same variables that the Slurm engine exports before the job script
            environment=_exported_environment(quanting_env.to_dict()),
            labels={
                JOB_LABEL: quanting_env.raw_file_id,
                OUTPUT_PATH_LABEL: str(internal_output_path),
            },
            # write output files with the same ownership as the worker would
            user=f"{os.getuid()}:0",
            mem_limit=quanting_env.slurm_mem.lower(),
            nano_cpus=quanting_env.slurm_cpus_per_task * NANO_CPUS_PER_CPU,
            # the quanting software must not need any network access
            network_mode="none",
        )

        return container.id[:12]

    def get_job_status(self, job_id: str) -> str:
        """Get the status of a job by inspecting the container."""
        container = self._get_container(job_id)
        if container is None:
            return JobStates.UNKNOWN

        return self._get_job_state(container)

    def get_job_result(self, job_id: str) -> tuple[str, int]:
        """Get the job status and execution time by inspecting the container."""
        container = self._get_container(job_id)
        if container is None:
            return JobStates.UNKNOWN, 0

        job_status = self._get_job_state(container)
        self._write_logs(container)

        state = _get_state(container)
        time_elapsed = _get_time_elapsed(
            state.get("StartedAt"), state.get("FinishedAt")
        )

        return job_status, time_elapsed

    def _get_image(self, image: str) -> str:
        """Check that an image is present on this host and return its reference.

        `containers.run()` would otherwise pull an unknown image from the registry, which would
        allow anyone who can create settings to run arbitrary images on the worker host.
        """
        try:
            self._client.images.get(image)
        except ImageNotFound as e:
            raise AirflowFailException(
                f"Image '{image}' is not present on this host. An administrator needs to build "
                f"or pull it first."
            ) from e

        return image

    def _to_host_path(self, internal_path: Path) -> Path:
        """Translate a path within the worker container to the corresponding host path.

        This trick enables to access the files on the container file system with the same paths as on the shared file system.

        E.g. /opt/airflow/mounts/output/P1/out_file.raw/custom
        -> /home/kraken-user/alphakraken/production/mounts/output/P1/out_file.raw/custom
        for `locations.general.mounts_path: /home/kraken-user/alphakraken/production/mounts`.
        """
        return self._host_mounts_path / internal_path.relative_to(
            InternalPaths.MOUNTS_PATH
        )

    def _get_container(self, job_id: str) -> Container | None:
        """Get the container with the given id, None if it does not exist (anymore)."""
        try:
            return self._client.containers.get(job_id)
        except NotFound:
            logging.warning(f"Container {job_id} not found.")
            return None

    def _remove_container(self, container_name: str) -> None:
        """Remove a container left over from a previous run of the same job, if any."""
        try:
            container = self._client.containers.get(container_name)
        except NotFound:
            return

        logging.info(f"Removing leftover container {container_name}.")
        container.remove(force=True)

    @staticmethod
    def _get_job_state(container: Container) -> str:
        """Map the state of a container to a job state."""
        state = _get_state(container)

        if (job_state := _DOCKER_STATE_TO_JOB_STATE.get(container.status)) is not None:
            return job_state

        if state.get("OOMKilled"):
            return JobStates.OUT_OF_MEMORY

        exit_code = state.get("ExitCode")
        if exit_code == 0:
            return JobStates.COMPLETED

        logging.info(f"Container exited with {exit_code=}")
        return JobStates.FAILED

    @staticmethod
    def _write_logs(container: Container) -> None:
        """Write the container logs to the log file in the output folder."""
        log_file_path = Path(container.labels[OUTPUT_PATH_LABEL]) / LOG_FILE_NAME
        with log_file_path.open("wb") as file:
            file.write(container.logs())
        logging.info(f"Wrote container logs to {log_file_path}")


def _to_container_name(name: str) -> str:
    """Replace all characters that docker does not accept in a container name."""
    return re.sub(_FORBIDDEN_CONTAINER_NAME_CHARACTERS_PATTERN, "_", name)


def _exported_environment(environment: dict[str, str]) -> dict[str, str]:
    """Get the variables to set in the container, ignoring keys with leading underscore."""
    return {k: str(v) for k, v in environment.items() if not k.startswith("_")}


def _get_state(container: Container) -> dict:
    """Get the `State` section of a container's attributes."""
    return (container.attrs or {}).get("State", {})


def _parse_docker_timestamp(timestamp: str) -> datetime:
    """Parse a docker timestamp, truncating its nanoseconds to the microseconds datetime supports."""
    return datetime.fromisoformat(
        re.sub(r"(\.\d{6})\d+", r"\1", timestamp.replace("Z", "+00:00"))
    )


def _get_time_elapsed(started_at: str | None, finished_at: str | None) -> int:
    """Get the number of seconds between two docker timestamps, 0 if not (yet) available."""
    if not started_at or not finished_at or finished_at.startswith(UNSET_TIMESTAMP):
        return 0

    started = _parse_docker_timestamp(started_at)
    finished = _parse_docker_timestamp(finished_at)
    return int((finished - started).total_seconds())
