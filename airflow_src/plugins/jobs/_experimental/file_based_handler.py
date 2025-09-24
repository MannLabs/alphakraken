"""Implementation of a file-based job handler that triggers jobs via file creation.

- Creates a `.job` file with environment variables to signal job start in the `output/job_queue` directory.
- Checks a `job_status.log` file in the job's output directory to determine job status.

Notes:
    - requires an external process to monitor the `job_queue` directory, execute jobs accordingly, and update the `job_status.log` file.
    - requires the bind mount of the output folder in docker-compose.yml to be of type "rw" (read-write), not "ro" (read-only).

"""

# TODO: add unit tests
import logging
from pathlib import Path

from airflow.exceptions import AirflowFailException
from common.keys import JobStates, QuantingEnv
from common.paths import get_internal_output_path_for_raw_file
from dags.impl.processor_impl import _get_project_id_or_fallback
from jobs.job_handler import JobHandler

from shared.db.interface import get_raw_file_by_id
from shared.keys import InternalPaths


class FileBasedJobHandler(JobHandler):
    """Implementation of JobHandler that triggers jobs by creating files."""

    def __init__(self):
        """Initialize the file-based job handler."""
        super().__init__()

        # is is a bit of a hack to use the output path here, but it avoids another bind mount
        self._job_submit_dir = (
            Path(InternalPaths.MOUNTS_PATH) / InternalPaths.OUTPUT / "job_queue"
        )

    def start_job(
        self,
        job_script_name: str,
        environment: dict[str, str],
        year_month_folder: str,
    ) -> str:
        """Start a job by writing quanting environment to a .job file.

        Args:
            job_script_name: Name of the job script (ignored for file-based handler)
            environment: Environment variables containing quanting configuration
            year_month_folder: Folder for job outputs (ignored for file-based handler)

        Returns:
            Job ID (raw_file_id from the environment)

        """
        del job_script_name  # unused
        del year_month_folder  # unused

        raw_file_id = environment[QuantingEnv.RAW_FILE_ID]
        try:
            self._job_submit_dir.mkdir(exist_ok=True)
        except OSError:
            logging.info(
                "For this job handler, you need to change the bind mount of the output folder in docker-compose.yml to type 'rw' (read-write)."
            )
            logging.exception("Failed to create job submit directory.")

        job_file_path = self._job_submit_dir / f"{raw_file_id}.job"

        if job_file_path.exists():
            raise AirflowFailException(
                f"Job file {job_file_path} already exists. Please remove it before starting a new job."
            )

        logging.info(f"Creating job file at {job_file_path}")

        with job_file_path.open("w") as f:
            for key, value in environment.items():
                if key in [
                    QuantingEnv.RAW_FILE_ID,
                    QuantingEnv.OUTPUT_PATH,
                    QuantingEnv.RELATIVE_OUTPUT_PATH,
                    QuantingEnv.CUSTOM_COMMAND,
                ]:
                    f.write(f"{key}={value}\n")

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

        project_id_or_fallback = _get_project_id_or_fallback(
            raw_file.project_id, raw_file.instrument_id
        )

        output_path = get_internal_output_path_for_raw_file(
            raw_file, project_id_or_fallback
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
