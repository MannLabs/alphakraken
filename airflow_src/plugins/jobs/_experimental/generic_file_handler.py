"""Experimental implementation of a generic job handler that doesn't use Slurm."""

# TODO: add unit tests
from common.keys import JobStates
from jobs.job_handler import JobHandler
from sensors.ssh_utils import ssh_execute


class GenericJobHandler(JobHandler):
    """Implementation of JobHandler that doesn't use Slurm but a more generic approach."""

    def start_job(self, quanting_env: dict[str, str], year_month_folder: str) -> str:
        """Start a quanting job on the generic job engine."""
        del quanting_env
        del year_month_folder
        command = "sleep 60"
        wrapped_command = f"{command} > /dev/null 2>&1 & echo $!"

        return ssh_execute(wrapped_command)

    def get_job_status(self, job_id: str) -> str:
        """Get the status of a job on the generic job engine."""
        command = f"ps -p {job_id} > /dev/null 2>&1; echo $?"

        ssh_return = ssh_execute(command)

        # here we can decide only on two states: running or not running
        return JobStates.RUNNING if ssh_return == "0" else JobStates.COMPLETED

    def get_job_result(self, job_id: str) -> tuple[str, int]:
        """Get the job status and time elapsed from the generic job engine."""
        return self.get_job_status(job_id), 0  # how to get the run time?
