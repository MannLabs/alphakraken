"""Implementation of a job handler that runs jobs on a Slurm cluster via SSH."""

import logging
from datetime import datetime
from pathlib import Path

from airflow.exceptions import AirflowFailException
from common.constants import CLUSTER_BASE_WORKING_DIR_NAME, DUMMY_TIME_ELAPSED
from common.keys import JobStates
from common.quanting_env import QuantingEnv
from jobs.job_handler import JobHandler
from sensors.ssh_utils import ssh_execute


class SlurmSSHJobHandler(JobHandler):
    """Implementation of JobHandler that executes commands on a Slurm cluster via SSH."""

    def __init__(self, cluster_base_dir: Path):
        """Initialize the Slurm job handler.

        Args:
            cluster_base_dir: Working directory on the cluster, holding the submit script
                and the job logs

        """
        super().__init__()
        self._cluster_base_dir = cluster_base_dir
        self._cluster_base_working_dir_path = (
            self._cluster_base_dir / CLUSTER_BASE_WORKING_DIR_NAME
        )

    def start_job(
        self,
        job_script_name: str,
        quanting_env: QuantingEnv,
        year_month_folder: str,
    ) -> str:
        """Start a job on the Slurm cluster via SSH."""
        command = (
            self._create_export_environment_cmd(quanting_env.to_dict())
            + "\n"
            + self._get_submit_job_cmd(job_script_name, quanting_env, year_month_folder)
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
        cmd = (
            self._check_job_result_cmd(job_id) + "\n" + self._get_job_state_cmd(job_id)
        )
        ssh_return = ssh_execute(cmd)
        time_elapsed = self._get_time_elapsed(ssh_return)
        job_status = ssh_return.split("\n")[-1]
        return job_status, time_elapsed

    def _get_submit_job_cmd(
        self, job_script_name: str, quanting_env: QuantingEnv, year_month_folder: str
    ) -> str:
        """Get the command to run the job on the cluster.

        Its last line of output to stdout must be the job id of the submitted job.
        ${JID##* } is removing everything up to the last space.

        :param job_script_name: the name of the slurm job script, e.g. "submit_job.sh"
        :param year_month_folder: the sub folder in which the slurm output script will be written to, e.g. "2024_07"
        """
        cluster_job_script_path = self._cluster_base_dir / job_script_name
        cluster_working_dir = self._cluster_base_working_dir_path / year_month_folder

        params = " ".join(
            [
                f"--cpus-per-task={quanting_env.slurm_cpus_per_task}",
                f"--mem={quanting_env.slurm_mem}",
                f"--time={quanting_env.slurm_time}",
            ]
        )

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
    def _check_job_result_cmd(job_id: str) -> str:
        """Shell command to print the elapsed time, sacct information and the contents of the slurm log file for a given job id.

        To reduce the number of ssh calls, we combine multiple commands into one
        In order to be able to extract the run time, we expect the first line to contain only that, e.g. "00:08:42"
        """
        return "\n".join(
            [
                f"SACCT_OUT=$(sacct -l --parsable2 -j {job_id} 2>/dev/null)",
                "if [ $? -eq 0 ]; then",
                f'echo "$SACCT_OUT" | awk -F\'|\' \'NR==1{{for(i=1;i<=NF;i++)if($i=="Elapsed")c=i}}END{{print (c && NR>1) ? $c : "{DUMMY_TIME_ELAPSED}"}}\'',
                'echo "$SACCT_OUT"',
                "else",
                f'echo "{DUMMY_TIME_ELAPSED}"',
                "fi",
                # f"cat {slurm_output_file_path}", # TODO: do this elsewhere
            ]
        )

    @staticmethod
    def _get_job_state_cmd(job_id: str) -> str:
        """Shell command to print the status of a job with a given job id.

        Uses `scontrol` to get the state and falls back to `sacct` (more expensive) only if
        `scontrol` returns an error (e.g. the job has been purged from the queue).
        If both fail to determine the state, "UNKNOWN" is returned.

        Its only output must be the job status.
        """
        return "\n".join(
            [
                f"JOB_INFO=$(scontrol show job {job_id} 2>/dev/null)",
                "if [ $? -eq 0 ]; then",
                "ST=$(echo \"$JOB_INFO\" | grep JobState | awk -F 'JobState=' '{print $2}' | awk -F ' ' '{print $1}')",
                "else",
                f"ST=$(sacct -j {job_id} -o State 2>/dev/null | awk 'FNR == 3 {{print $1}}')",
                "fi",
                f'echo "${{ST:-{JobStates.UNKNOWN}}}"',
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
        """Extract the time in seconds from a string "hours:minutes:seconds" in the first line of a string.

        Returns 0 if the first line is not a timestamp, e.g. because `sacct` is not available.
        """
        time_stamp = ssh_return.split("\n")[0]
        logging.info(f"extracted {time_stamp=}")
        try:
            t = datetime.strptime(time_stamp, "%H:%M:%S")  # noqa: DTZ007
        except ValueError:
            logging.warning(f"Could not parse time elapsed from {time_stamp=}")
            return 0
        return (t.hour * 3600) + (t.minute * 60) + t.second
