"""Module containing the commands to interact with the SLURM cluster.

All the commands must be bash script that are executable on the cluster.
"""

from common.settings import CLUSTER_JOB_SCRIPT_PATH, CLUSTER_WORKING_DIR


# TODO: how to bring 'submit_job.sh' to the cluster?
def get_run_quanting_cmd() -> str:
    """Get the command to run the quanting job on the cluster.

    Its last line of output to stdout must be the job id of the submitted job.
    ${JID##* } is removing everything up to the last space.
    """
    return f"""
cd {CLUSTER_WORKING_DIR}
cat {CLUSTER_JOB_SCRIPT_PATH}
JID=$(sbatch {CLUSTER_JOB_SCRIPT_PATH})
echo ${{JID##* }}
"""


def get_job_info_cmd(job_id: str, slurm_output_file: str) -> str:
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
