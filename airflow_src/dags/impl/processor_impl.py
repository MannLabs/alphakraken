"""Business logic for the acquisition_processor."""

import json
import logging
from datetime import datetime
from pathlib import Path
from random import random

from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance
from cluster_scripts.slurm_commands import (
    get_job_info_cmd,
    get_job_state_cmd,
    get_run_quanting_cmd,
)
from common.keys import (
    AirflowVars,
    DagContext,
    DagParams,
    JobStates,
    OpArgs,
    QuantingEnv,
    XComKeys,
)
from common.settings import (
    CLUSTER_WORKING_DIR,
    FALLBACK_PROJECT_ID,
    InternalPaths,
    get_internal_output_path,
    get_output_folder_rel_path,
)
from common.utils import get_airflow_variable, get_env_variable, get_xcom, put_xcom
from impl.project_id_handler import get_unique_project_id
from metrics.metrics_calculator import calc_metrics
from sensors.ssh_sensor import SSHSensorOperator

from shared.db.interface import (
    add_metrics_to_raw_file,
    get_all_project_ids,
    get_settings_for_project,
    update_raw_file,
)
from shared.db.models import RawFileStatus
from shared.keys import EnvVars


def _get_project_id_for_raw_file(raw_file_name: str) -> str:
    """Get the project id for a raw file or the fallback ID if not present."""
    all_project_ids = get_all_project_ids()
    unique_project_id = get_unique_project_id(raw_file_name, all_project_ids)
    return unique_project_id if unique_project_id is not None else FALLBACK_PROJECT_ID


def prepare_quanting(ti: TaskInstance, **kwargs) -> None:
    """Prepare the environmental variables for the quanting job."""
    raw_file_name = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_NAME]
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]

    project_id = _get_project_id_for_raw_file(raw_file_name)

    io_pool_folder = get_env_variable(EnvVars.IO_POOL_FOLDER)
    instrument_subfolder = f"{io_pool_folder}/{InternalPaths.BACKUP}/{instrument_id}"

    output_folder_rel_path = get_output_folder_rel_path(raw_file_name, project_id)

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
        QuantingEnv.OUTPUT_FOLDER_REL_PATH: str(output_folder_rel_path),
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

    # upfront check 1
    if (job_id := get_xcom(ti, XComKeys.JOB_ID, -1)) != -1:
        logging.warning(f"Job already started with {job_id}, skipping.")
        return

    # upfront check 2
    output_path = get_internal_output_path(
        quanting_env[QuantingEnv.RAW_FILE_NAME],
        project_id=quanting_env[QuantingEnv.PROJECT_ID],
    )
    if Path(output_path).exists():
        msg = f"Output path {output_path} already exists."
        if get_airflow_variable(AirflowVars.ALLOW_OUTPUT_OVERWRITE, "False") == "True":
            logging.warning(
                f"{msg} Overwriting it because ALLOW_OUTPUT_OVERWRITE is set."
            )
        else:
            raise AirflowFailException(
                f"{msg} Remove it before restarting the quanting or set ALLOW_OUTPUT_OVERWRITE."
            )

    command = _create_export_command(quanting_env) + get_run_quanting_cmd()
    logging.info(f"Running command: >>>>\n{command}\n<<<< end of command")

    ssh_return = SSHSensorOperator.ssh_execute(command, ssh_hook)

    try:
        job_id = str(int(ssh_return.split("\n")[-1]))
    except Exception as e:
        logging.exception("Did not get a valid job id from the cluster.")
        raise AirflowFailException from e

    update_raw_file(
        quanting_env[QuantingEnv.RAW_FILE_NAME], new_status=RawFileStatus.QUANTING
    )

    put_xcom(ti, XComKeys.JOB_ID, job_id)


def get_business_errors(raw_file_name: str, project_id: str) -> list[str]:
    """Extract business errors from the alphaDIA output."""

    def _extract_error_codes(events_jsonl_file_path: Path) -> list[str]:
        """Extract the error codes from the events.jsonl file."""
        error_codes = []
        with events_jsonl_file_path.open() as file:
            for line in file:
                try:
                    data = json.loads(line.strip())
                    if (
                        data.get("name") == "exception"
                        and "error_code" in data
                        and data["error_code"] != ""
                    ):
                        error_codes.append(data["error_code"])
                except json.JSONDecodeError:  # noqa: PERF203
                    logging.warning(f"Skipping invalid JSON: {line.strip()}")
        return error_codes

    output_path = get_internal_output_path(raw_file_name, project_id)

    events_jsonl_path = output_path / ".progress" / raw_file_name / "events.jsonl"

    error_codes = []
    try:
        error_codes = _extract_error_codes(events_jsonl_path)
    except FileNotFoundError:
        logging.warning(f"Could not find {events_jsonl_path=}")
    return error_codes


def get_job_info(ti: TaskInstance, **kwargs) -> bool:
    """Get info (slurm log, alphaDIA log) about a job from the cluster.

    Return False in case downstream tasks should be skipped, True otherwise.
    """
    ssh_hook = kwargs[OpArgs.SSH_HOOK]
    job_id = get_xcom(ti, XComKeys.JOB_ID)

    slurm_output_file = f"{CLUSTER_WORKING_DIR}/slurm-{job_id}.out"
    cmd = get_job_info_cmd(job_id, slurm_output_file) + get_job_state_cmd(job_id)
    ssh_return = SSHSensorOperator.ssh_execute(cmd, ssh_hook)

    time_elapsed = _get_time_elapsed(ssh_return)
    put_xcom(ti, XComKeys.QUANTING_TIME_ELAPSED, time_elapsed)

    job_status = ssh_return.split("\n")[-1]
    logging.info(f"Job {job_id} exited with status {job_status}.")

    # now check for errors
    if job_status == JobStates.FAILED:
        quanting_env = get_xcom(ti, XComKeys.QUANTING_ENV)
        raw_file_name = quanting_env[QuantingEnv.RAW_FILE_NAME]
        project_id = quanting_env[QuantingEnv.PROJECT_ID]

        if len(business_errors := get_business_errors(raw_file_name, project_id)):
            update_raw_file(
                raw_file_name,
                new_status=RawFileStatus.QUANTING_FAILED,
                status_details=";".join(business_errors),
            )
            return False  # skip downstream tasks

    if job_status != JobStates.COMPLETED:  # this implicitly covers non-business error
        logging.info(f"Job {job_id} exited with status {job_status}.")
        raise AirflowFailException(f"Quanting failed: {job_status=}")

    return True


def _get_time_elapsed(ssh_return: str) -> int:
    """Extract the time in seconds from a string "hours:minutes:seconds" in the first line of a string."""
    time_stamp = ssh_return.split("\n")[0]
    logging.info(f"extracted {time_stamp=}")
    t = datetime.strptime(time_stamp, "%H:%M:%S")  # noqa: DTZ007
    return (t.hour * 3600) + (t.minute * 60) + t.second


def compute_metrics(ti: TaskInstance, **kwargs) -> None:
    """Compute metrics from the quanting results."""
    del kwargs

    quanting_env = get_xcom(ti, XComKeys.QUANTING_ENV)

    output_path = get_internal_output_path(
        quanting_env[QuantingEnv.RAW_FILE_NAME], quanting_env[QuantingEnv.PROJECT_ID]
    )
    metrics = calc_metrics(output_path)

    put_xcom(ti, XComKeys.METRICS, metrics)


def upload_metrics(ti: TaskInstance, **kwargs) -> None:
    """Upload the metrics to the database."""
    del kwargs

    raw_file_name = get_xcom(ti, XComKeys.RAW_FILE_NAME)
    metrics = get_xcom(ti, XComKeys.METRICS)

    metrics["quanting_time_elapsed"] = get_xcom(ti, XComKeys.QUANTING_TIME_ELAPSED)

    add_metrics_to_raw_file(raw_file_name, metrics)

    update_raw_file(raw_file_name, new_status=RawFileStatus.DONE)
