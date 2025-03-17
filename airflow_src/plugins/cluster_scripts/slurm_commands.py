"""Module containing the commands to interact with the SLURM cluster.

All the commands must be bash script that are executable on the cluster.
"""

import logging
from datetime import datetime

from airflow.exceptions import AirflowFailException
from common.constants import CLUSTER_JOB_SCRIPT_PATH, CLUSTER_WORKING_DIR
from sensors.ssh_utils import ssh_execute


# TODO: how to bring 'submit_job.sh' to the cluster?
def get_run_quanting_cmd(year_month_folder: str) -> str:
    """Get the command to run the quanting job on the cluster.

    Its last line of output to stdout must be the job id of the submitted job.
    ${JID##* } is removing everything up to the last space.

    :param year_month_folder: the sub folder in which the slurm output script will be written to, e.g. "2024_07"
    """
    return f"""
mkdir -p {CLUSTER_WORKING_DIR}/{year_month_folder}
cd {CLUSTER_WORKING_DIR}/{year_month_folder}
cat {CLUSTER_JOB_SCRIPT_PATH}
JID=$(sbatch {CLUSTER_JOB_SCRIPT_PATH})
echo ${{JID##* }}
"""


def check_quanting_result_cmd(job_id: str, slurm_output_file: str) -> str:
    """Get the job info for a given job id.

    To reduce the number of ssh calls, we combine multiple commands into one
    In order to be able to extract the run time, we expect the first line to contain only that, e.g. "00:08:42"
    """
    return f"""TIME_ELAPSED=$(sacct --format=Elapsed -j {job_id} | tail -n 1); echo $TIME_ELAPSED
sacct -l -j {job_id}
cat {slurm_output_file}
"""


def get_job_state_cmd(job_id: str) -> str:
    """Get the state of a job with a given job id.

    Its only output must be the job status.
    """
    return f"""
ST=$(sacct -j {job_id} -o State | awk 'FNR == 3 {{print $1}}')
echo $ST
"""


def _create_export_command(mapping: dict[str, str]) -> str:
    """Create a bash command to export environment variables."""
    return "\n".join([f"export {k}={v}" for k, v in mapping.items()])


# TODO: dedicated test
def ssh_slurm_start_job(quanting_env: dict[str, str], year_month_folder: str) -> str:
    """Start a quanting job on the SLURM cluster via SSH."""
    command = _create_export_command(quanting_env) + get_run_quanting_cmd(
        year_month_folder
    )
    logging.info(f"Running command: >>>>\n{command}\n<<<< end of command")
    ssh_return = ssh_execute(command)

    try:
        job_id = str(int(ssh_return.split("\n")[-1]))
    except Exception as e:
        logging.exception("Did not get a valid job id from the cluster.")
        raise AirflowFailException("Job submission failed.") from e

    return job_id


# TODO: dedicated test
def _get_time_elapsed(ssh_return: str) -> int:
    """Extract the time in seconds from a string "hours:minutes:seconds" in the first line of a string."""
    time_stamp = ssh_return.split("\n")[0]
    logging.info(f"extracted {time_stamp=}")
    t = datetime.strptime(time_stamp, "%H:%M:%S")  # noqa: DTZ007
    return (t.hour * 3600) + (t.minute * 60) + t.second


# TODO: dedicated test
def ssh_slurm_get_job_result(job_id: str) -> tuple[str, int]:
    """Get the job status and time elapsed from the SLURM cluster via SSH."""
    # the wildcard here is a bit of a hack to avoid retrieving the year_month
    # subfolder here .. should be no problem if job_ids are unique
    slurm_output_file = f"{CLUSTER_WORKING_DIR}/*/slurm-{job_id}.out"
    cmd = check_quanting_result_cmd(job_id, slurm_output_file) + get_job_state_cmd(
        job_id
    )
    ssh_return = ssh_execute(cmd)
    time_elapsed = _get_time_elapsed(ssh_return)
    job_status = ssh_return.split("\n")[-1]
    return job_status, time_elapsed
