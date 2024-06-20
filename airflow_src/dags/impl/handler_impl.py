"""Business logic for the acquisition_handler."""

import logging
from datetime import datetime
from random import random

from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance
from cluster_scripts.slurm_commands import get_job_info_cmd, get_run_quanting_cmd
from common.keys import DagContext, DagParams, EnvVars, OpArgs, QuantingEnv, XComKeys
from common.settings import (
    CLUSTER_WORKING_DIR,
    FALLBACK_PROJECT_ID,
    get_internal_output_path,
    get_output_folder_name,
    get_relative_instrument_data_path,
)
from common.utils import get_env_variable, get_xcom, put_xcom
from impl.project_id_handler import get_unique_project_id
from metrics.metrics_calculator import calc_metrics
from sensors.ssh_sensor import SSHSensorOperator

from shared.db.interface import (
    add_metrics_to_raw_file,
    get_all_project_ids,
    get_settings_for_project,
    update_raw_file_status,
)
from shared.db.models import RawFileStatus


def _get_project_id_for_raw_file(raw_file_name: str) -> str:
    """Get the project id for a raw file or the fallback ID if not present."""
    all_project_ids = get_all_project_ids()
    unique_project_id = get_unique_project_id(raw_file_name, all_project_ids)
    return unique_project_id if unique_project_id is not None else FALLBACK_PROJECT_ID


def prepare_quanting(ti: TaskInstance, **kwargs) -> None:
    """Prepare the environmental variables for the quanting job."""
    raw_file_name = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_NAME]
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]

    instrument_subfolder = get_relative_instrument_data_path(instrument_id)
    output_folder_name = get_output_folder_name(raw_file_name)

    project_id = _get_project_id_for_raw_file(raw_file_name)

    settings = get_settings_for_project(project_id)

    # TODO: remove random speclib file hack
    # this is so hacky it makes my head hurt:
    # currently, two instances of alphaDIA compete for locking the speclib file
    # cf. https://github.com/MannLabs/alphabase/issues/180
    # This reduces the chance for this to happen by 90%
    speclib_file_name = f"{int(random()*10)}_{settings.speclib_file_name}"  # noqa: S311

    io_pool_folder = get_env_variable(EnvVars.IO_POOL_FOLDER)

    quanting_env = {
        QuantingEnv.RAW_FILE_NAME: raw_file_name,
        QuantingEnv.INSTRUMENT_SUBFOLDER: instrument_subfolder,
        QuantingEnv.OUTPUT_FOLDER_NAME: output_folder_name,
        QuantingEnv.SPECLIB_FILE_NAME: speclib_file_name,
        QuantingEnv.FASTA_FILE_NAME: settings.fasta_file_name,
        QuantingEnv.CONFIG_FILE_NAME: settings.config_file_name,
        QuantingEnv.SOFTWARE: settings.software,
        QuantingEnv.PROJECT_ID: project_id,
        QuantingEnv.IO_POOL_FOLDER: io_pool_folder,
    }

    put_xcom(ti, XComKeys.QUANTING_ENV, quanting_env)
    # this is redundant to the entry in QUANTING_ENV, but makes downstream access a bit more convenient
    put_xcom(ti, XComKeys.RAW_FILE_NAME, raw_file_name)


def _create_export_command(mapping: dict[str, str]) -> str:
    """Create a bash command to export environment variables."""
    return "\n".join([f"export {k}={v}" for k, v in mapping.items()])


def run_quanting(ti: TaskInstance, **kwargs) -> None:
    """Run the quanting job on the cluster."""
    # IMPLEMENT:
    # wait for the cluster to be ready (20% idling) -> dedicated (sensor) task, mind race condition! (have pool = 1 for that)

    quanting_env = get_xcom(ti, XComKeys.QUANTING_ENV)
    ssh_hook = kwargs[OpArgs.SSH_HOOK]

    command = _create_export_command(quanting_env) + get_run_quanting_cmd()
    logging.info(f"Running command: >>>>\n{command}\n<<<< end of command")

    # TODO: prevent re-starting the same job again, by either
    #  - give a unique job name and search latest history
    #  - check if job_id is already set (weak!)
    #  - check if output folder already exists
    ssh_return = SSHSensorOperator.ssh_execute(command, ssh_hook)

    try:
        job_id = str(int(ssh_return.split("\n")[-1]))
    except Exception as e:
        logging.exception("Did not get a valid job id from the cluster.")
        raise AirflowFailException from e

    update_raw_file_status(
        quanting_env[QuantingEnv.RAW_FILE_NAME], RawFileStatus.PROCESSING
    )

    put_xcom(ti, XComKeys.JOB_ID, job_id)


def get_job_info(ti: TaskInstance, **kwargs) -> None:
    """Get info (slurm log, alphaDIA log) about a job from the cluster."""
    ssh_hook = kwargs[OpArgs.SSH_HOOK]
    job_id = get_xcom(ti, XComKeys.JOB_ID)

    slurm_output_file = f"{CLUSTER_WORKING_DIR}/slurm-{job_id}.out"

    cmd = get_job_info_cmd(job_id, slurm_output_file)
    ssh_return = SSHSensorOperator.ssh_execute(cmd, ssh_hook)

    logging.info(ssh_return)

    time_elapsed = _get_time_elapsed(ssh_return)

    put_xcom(ti, XComKeys.TIME_ELAPSED, time_elapsed)


def _get_time_elapsed(ssh_return: str) -> int:
    """Extract the time in seconds from a string "hours:minutes:seconds" in the first line of a string."""
    time_stamp = ssh_return.split("\n")[0]
    logging.info(f"extracted {time_stamp=}")
    t = datetime.strptime(time_stamp, "%H:%M:%S")  # noqa: DTZ007
    return (t.hour * 3600) + (t.minute * 60) + t.second


def compute_metrics(ti: TaskInstance, **kwargs) -> None:
    """Compute metrics from the quanting results."""
    del kwargs

    raw_file_name = get_xcom(ti, XComKeys.RAW_FILE_NAME)

    output_path = get_internal_output_path(raw_file_name)
    metrics = calc_metrics(output_path)

    put_xcom(ti, XComKeys.METRICS, metrics)


def upload_metrics(ti: TaskInstance, **kwargs) -> None:
    """Upload the metrics to the database."""
    del kwargs

    raw_file_name = get_xcom(ti, XComKeys.RAW_FILE_NAME)
    metrics = get_xcom(ti, XComKeys.METRICS)

    time_elapsed = get_xcom(ti, XComKeys.TIME_ELAPSED)
    metrics["time_elapsed"] = time_elapsed

    add_metrics_to_raw_file(raw_file_name, metrics)

    update_raw_file_status(raw_file_name, RawFileStatus.PROCESSED)
