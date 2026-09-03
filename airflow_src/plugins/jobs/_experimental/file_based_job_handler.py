"""Implementation of a file-based job handler that triggers jobs via file creation.

- Creates a `.job` file with environment variables to signal job start in the `output/job_queue` directory.
- Checks a `job_status.log` file in the job's output directory to determine job status.

Notes:
    - requires an external process to monitor the `job_queue` directory, execute jobs accordingly, and update the `job_status.log` file.
        See the `misc/job_queue_watcher/job_queue_watcher.py` script for an example implementation.
    - requires the bind mount of the output folder in docker-compose.yml to be of type "rw" (read-write), not "ro" (read-only).
    - requires key 'locations.software.absolute_path: ""' in alphakraken.{env}.yaml

"""

# TODO: add unit tests
import logging
from pathlib import Path

from airflow.exceptions import AirflowFailException
from common.keys import JobStates
from common.paths import get_internal_output_path_for_raw_file
from common.quanting_env import QuantingEnv
from jobs.job_handler import JobHandler

from shared.db.interface import get_raw_file_by_id
from shared.keys import InternalPaths, SoftwareTypes


class FileBasedJobHandler(JobHandler):
    """Implementation of JobHandler that triggers jobs by creating files."""

    def __init__(self):
        """Initialize the file-based job handler."""
        super().__init__()

        # is is a bit of a hack to use the output path here, but it avoids another bind mount
        self._job_submit_dir = (
            Path(InternalPaths.MOUNTS_PATH) / InternalPaths.OUTPUT / "job_queue"
        )

    def start_job(self, quanting_env: QuantingEnv) -> str:
        """Start a job by writing quanting environment to a .job file.

        Args:
            quanting_env: Environment of the job to submit

        Returns:
            Job ID (in the case of this handler, it's the raw file id)

        """
        raw_file_id = quanting_env.raw_file_id
        job_file_path = self._job_submit_dir / f"{raw_file_id}.job"

        if job_file_path.exists():
            raise AirflowFailException(
                f"Job file {job_file_path} already exists. Please remove it before starting a new job."
            )

        logging.info(f"Creating job file at {job_file_path}")

        try:
            self._job_submit_dir.mkdir(exist_ok=True)

            # this file format is read by an external watcher, cf. the module docstring
            with job_file_path.open("w") as f:
                f.write(f"RAW_FILE_ID={quanting_env.raw_file_id}\n")
                f.write(f"OUTPUT_PATH={quanting_env.output_path}\n")
                f.write(f"RELATIVE_OUTPUT_PATH={quanting_env.relative_output_path}\n")
                f.write(f"CUSTOM_COMMAND={quanting_env.custom_command}\n")
        except OSError as e:
            logging.info(
                "For this job handler, you need to change the bind mount of the output folder in docker-compose.yml to type 'rw' (read-write)."
            )
            logging.exception("Failed to create job submit directory.")
            raise AirflowFailException from e

        logging.info(f"Job file created for raw_file_id: {raw_file_id}")
        return raw_file_id

    def get_job_status(self, job_id: str) -> str:
        """Get the status of a job by checking the job_status.log file.

        Args:
            job_id: Job ID (raw_file_id)

        Returns:
            Job status string (PENDING, RUNNING, COMPLETED, FAILED)

        """
        raw_file = get_raw_file_by_id(job_id)

        output_path = get_internal_output_path_for_raw_file(
            raw_file,
            software_type=SoftwareTypes.CUSTOM,  # this assumption makes life much easier, and should not limit flexibility too much
        )
        status_file = output_path / "job_status.log"

        if not status_file.exists():
            return JobStates.PENDING

        # Read the last non-empty line to get status
        with status_file.open("r") as f:
            lines = f.readlines()

        if not lines:
            return JobStates.RUNNING

        last_line = ""
        for line in reversed(lines):
            stripped_line = line.strip()
            if stripped_line:
                last_line = stripped_line
                break

        if last_line == "COMPLETED":
            return JobStates.COMPLETED
        if last_line == "FAILED":
            return JobStates.FAILED
        return JobStates.RUNNING

    def get_job_result(self, job_id: str) -> tuple[str, int]:
        """Get the job status and execution time.

        Args:
            job_id: Job ID (raw_file_id)

        Returns:
            Tuple of (job_status, time_elapsed_seconds)

        """
        status = self.get_job_status(job_id)

        # For now, return 0 for elapsed time
        # Could be enhanced to parse timestamps from job_status.log
        elapsed_time = 0

        return status, elapsed_time
