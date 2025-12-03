"""Module containing the commands to interact with job clusters.

This module provides an abstract interface for job execution on different engines,
with concrete implementations for Slurm.
"""

import abc
import logging
from datetime import datetime

from airflow.exceptions import AirflowFailException
from common.constants import CLUSTER_BASE_WORKING_DIR_NAME
from common.keys import QuantingEnv
from sensors.ssh_utils import ssh_execute

from shared.yamlsettings import YAMLSETTINGS, YamlKeys, get_path

# TODO: move to settings
ENGINE: str = (
    YAMLSETTINGS.get(YamlKeys.GENERAL, {})
    .get(YamlKeys.JOB_ENGINE, {})
    .get(YamlKeys.TYPE, "slurm")
)


def _get_job_handler(engine: str | None = None) -> "JobHandler":
    """Factory function to get the appropriate job handler based on the configured engine."""
    engine = engine or ENGINE

    if engine == "generic":
        from jobs._experimental.generic_job_handler import GenericJobHandler

        logging.info("Using GenericJobHandler")
        return GenericJobHandler()
    if engine == "file_based":
        from jobs._experimental.file_based_job_handler import FileBasedJobHandler

        logging.info("Using FileBasedJobHandler")
        return FileBasedJobHandler()
    # Default to Slurm
    logging.info("Using SlurmSSHJobHandler")
    return SlurmSSHJobHandler()


class JobHandler(abc.ABC):
    """Abstract base class for job handling."""

    @abc.abstractmethod
    def start_job(
        self, job_script_name: str, environment: dict[str, str], year_month_folder: str
    ) -> str:
        """Start a job and return the job ID.

        Args:
            job_script_name: Name of the Slurm job script to run, e.g. "submit_job.sh"
            environment: Environment variables to set before job submission
            year_month_folder: Folder to store job outputs, e.g. "2024_07"

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


class SlurmSSHJobHandler(JobHandler):
    """Implementation of JobHandler that executes commands on a Slurm cluster via SSH."""

    def __init__(self):
        """Initialize the Slurm job handler."""
        super().__init__()
        self._cluster_base_dir = get_path(YamlKeys.Locations.SLURM)
        self._cluster_base_working_dir_path = (
            self._cluster_base_dir / CLUSTER_BASE_WORKING_DIR_NAME
        )

    def start_job(
        self,
        job_script_name: str,
        environment: dict[str, str],
        year_month_folder: str,
    ) -> str:
        """Start a job on the Slurm cluster via SSH."""
        command = (
            self._create_export_environment_cmd(environment)
            + "\n"
            + self._get_run_job_cmd(job_script_name, environment, year_month_folder)
        )
        logging.info(f"Running command: >>>>\n{command}\n<<<< end of command")
        ssh_return = ssh_execute(command)

        try:
            job_id = str(int(ssh_return.split("\n")[-1]))
        except Exception as e:
            logging.exception("Did not get a valid job id from the cluster.")
            # TODO: only DAG impl should raise AirflowFailException
            raise AirflowFailException("Job submission failed.") from e

        return job_id

    def get_job_status(self, job_id: str) -> str:
        """Get the status of a job on the Slurm cluster via SSH."""
        cmd = self._get_job_state_cmd(job_id)
        return ssh_execute(cmd)

    def get_job_result(self, job_id: str) -> tuple[str, int]:
        """Get the job status and time elapsed from the Slurm cluster via SSH."""
        # Use a wildcard path to find the output file without needing to know the specific year_month subfolder
        # This works as long as job_ids are unique across all subfolders
        slurm_output_file = (
            f"{self._cluster_base_working_dir_path}/*/slurm-{job_id}.out"
        )
        cmd = (
            self._check_job_result_cmd(job_id, slurm_output_file)
            + "\n"
            + self._get_job_state_cmd(job_id)
        )
        ssh_return = ssh_execute(cmd)
        time_elapsed = self._get_time_elapsed(ssh_return)
        job_status = ssh_return.split("\n")[-1]
        return job_status, time_elapsed

    def _get_run_job_cmd(
        self, job_script_name: str, environment: dict[str, str], year_month_folder: str
    ) -> str:
        """Get the command to run the job on the cluster.

        Its last line of output to stdout must be the job id of the submitted job.
        ${JID##* } is removing everything up to the last space.

        :param job_script_name: the name of the slurm job script, e.g. "submit_job.sh"
        :param year_month_folder: the sub folder in which the slurm output script will be written to, e.g. "2024_07"
        """
        cluster_job_script_path = self._cluster_base_dir / job_script_name
        cluster_working_dir = self._cluster_base_working_dir_path / year_month_folder

        # if those parameters are not passed, the value defined in the submit script are taken
        param_list = []
        if (cpus := environment.get(QuantingEnv.SLURM_CPUS_PER_TASK)) is not None:
            param_list.append(f"--cpus-per-task={cpus}")
        if (mem := environment.get(QuantingEnv.SLURM_MEM)) is not None:
            param_list.append(f"--mem={mem}")
        if (time := environment.get(QuantingEnv.SLURM_TIME)) is not None:
            param_list.append(f"--time={time}")
        params = " ".join(param_list)

        return "\n".join(
            [
                f"mkdir -p {cluster_working_dir}",
                f"cd {cluster_working_dir}",
                f"cat {cluster_job_script_path}",
                f"JID=$(sbatch {params} {cluster_job_script_path})",
                "echo ${JID##* }",
            ]
        )

    @staticmethod
    def _check_job_result_cmd(job_id: str, slurm_output_file: str) -> str:
        """Get the job info for a given job id.

        To reduce the number of ssh calls, we combine multiple commands into one
        In order to be able to extract the run time, we expect the first line to contain only that, e.g. "00:08:42"
        """
        return "\n".join(
            [
                f"TIME_ELAPSED=$(sacct --format=Elapsed -j {job_id} | tail -n 1); echo $TIME_ELAPSED",
                f"sacct -l -j {job_id}",
                f"cat {slurm_output_file}",
            ]
        )

    @staticmethod
    def _get_job_state_cmd(job_id: str) -> str:
        """Get the state of a job with a given job id.

        Its only output must be the job status.
        """
        return "\n".join(
            [
                f"ST=$(sacct -j {job_id} -o State | awk 'FNR == 3 {{print $1}}')",
                "echo $ST",
            ]
        )

    @staticmethod
    def _create_export_environment_cmd(mapping: dict[str, str]) -> str:
        """Create a bash command to export environment variables, ignoring keys with leading underscore."""
        return "\n".join(
            [f'export {k}="{v}"' for k, v in mapping.items() if not k.startswith("_")]
        )

    @staticmethod
    def _get_time_elapsed(ssh_return: str) -> int:
        """Extract the time in seconds from a string "hours:minutes:seconds" in the first line of a string."""
        time_stamp = ssh_return.split("\n")[0]
        logging.info(f"extracted {time_stamp=}")
        t = datetime.strptime(time_stamp, "%H:%M:%S")  # noqa: DTZ007
        return (t.hour * 3600) + (t.minute * 60) + t.second


def start_job(
    job_script_name: str, environment: dict[str, str], year_month_folder: str
) -> str:
    """Start a job using the configured job engine.

    Delegates to JobHandler.start_job(), see docs there.
    """
    handler = _get_job_handler()
    return handler.start_job(job_script_name, environment, year_month_folder)


def get_job_status(job_id: str) -> str:
    """Get the job status using the configured job engine.

    Delegates to JobHandler.get_job_status(), see docs there.
    """
    handler = _get_job_handler()
    return handler.get_job_status(job_id)


def get_job_result(job_id: str) -> tuple[str, int]:
    """Get the job status and time elapsed using the configured job engine."""
    handler = _get_job_handler()
    return handler.get_job_result(job_id)
