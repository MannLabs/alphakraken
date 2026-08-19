"""Implementation of a job handler that runs jobs in Docker containers on the Kraken host.

This is the execution engine for standalone deployments that have no external compute
resources: instead of submitting to Slurm, the Airflow worker starts one sibling container
per job via the Docker socket.

Notes:
    - requires the bind mount of the docker socket in docker-compose.yaml (cf. `group_add`).
    - requires key 'locations.general.mounts_path' in alphakraken.{env}.yaml to point to the
      mounts folder as seen by the docker host.
    - the image is resolved from the software type, cf. SOFTWARE_TYPE_TO_DOCKER_IMAGE.
    - `_SLURM_TIME` is not honored: docker has no wall clock limit.

"""

import logging
import os
import re
from datetime import datetime
from pathlib import Path

import docker
from airflow.exceptions import AirflowFailException
from common.keys import JobStates, QuantingEnv
from docker.errors import ImageNotFound, NotFound
from docker.models.containers import Container
from jobs.job_handler import JobHandler

from shared.keys import SOFTWARE_TYPE_TO_DOCKER_IMAGE, InternalPaths
from shared.yamlsettings import get_host_mounts_path

CONTAINER_NAME_PREFIX = "kraken"

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
    """Implementation of JobHandler that runs jobs in Docker containers on the Kraken host."""

    def __init__(self):
        """Initialize the docker job handler."""
        super().__init__()
        self._client = docker.from_env()
        self._host_mounts_path = get_host_mounts_path()

    def start_job(
        self,
        job_script_name: str,
        environment: dict[str, str],
        year_month_folder: str,
    ) -> str:
        """Start a job by running a container on the Kraken host.

        Args:
            job_script_name: Name of the job script (ignored for this handler)
            environment: Environment variables containing quanting configuration
            year_month_folder: Folder for job outputs (ignored for this handler)

        Returns:
            Job ID (in the case of this handler, the short container id)

        """
        del job_script_name  # unused
        del year_month_folder  # unused

        software_type = environment[QuantingEnv.SOFTWARE_TYPE]
        if (image := SOFTWARE_TYPE_TO_DOCKER_IMAGE.get(software_type)) is None:
            raise AirflowFailException(
                f"No docker image defined for software type '{software_type}'. "
                f"Known: {sorted(SOFTWARE_TYPE_TO_DOCKER_IMAGE)}"
            )

        raw_file_path = Path(environment[QuantingEnv.INTERNAL_RAW_FILE_PATH])
        output_path = Path(environment[QuantingEnv.INTERNAL_OUTPUT_PATH])
        for path in (raw_file_path, output_path):
            if not path.exists():
                raise AirflowFailException(f"Path {path} does not exist in the worker.")

        container_name = (
            f"{CONTAINER_NAME_PREFIX}-{software_type}-"
            f"{environment[QuantingEnv.RAW_FILE_ID]}"
        )
        self._remove_container(container_name)

        # the container sees the same paths as the worker, so no path translation is required
        # for anything that is passed to the software
        volumes = {
            str(self._to_host_path(raw_file_path)): {
                "bind": str(raw_file_path),
                "mode": "ro",
            },
            str(self._to_host_path(output_path)): {
                "bind": str(output_path),
                "mode": "rw",
            },
        }

        logging.info(f"Running image {image} as {container_name} with {volumes=}")

        try:
            container = self._client.containers.run(
                image,
                detach=True,
                name=container_name,
                volumes=volumes,
                environment={
                    QuantingEnv.RAW_FILE_PATH: str(raw_file_path),
                    QuantingEnv.OUTPUT_PATH: str(output_path),
                    QuantingEnv.NUM_THREADS: str(environment[QuantingEnv.NUM_THREADS]),
                },
                labels={
                    JOB_LABEL: environment[QuantingEnv.RAW_FILE_ID],
                    OUTPUT_PATH_LABEL: str(output_path),
                },
                # write output files with the same ownership as the worker would
                user=f"{os.getuid()}:0",
                mem_limit=str(environment[QuantingEnv.SLURM_MEM]).lower(),
                nano_cpus=int(environment[QuantingEnv.SLURM_CPUS_PER_TASK])
                * NANO_CPUS_PER_CPU,
                # the quanting software must not need any network access
                network_mode="none",
            )
        except ImageNotFound as e:
            raise AirflowFailException(
                f"Image {image} not found. Build it with "
                f"`./compose.sh --profile msqc build`."
            ) from e

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

    def _to_host_path(self, internal_path: Path) -> Path:
        """Translate a path within the worker container to the corresponding host path."""
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
