"""Business logic for the acquisition_processor."""

import json
import logging
from datetime import datetime
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance
from cluster_scripts.slurm_commands import (
    check_quanting_result_cmd,
    get_job_state_cmd,
    get_run_quanting_cmd,
)
from common.constants import (
    CLUSTER_WORKING_DIR,
    ERROR_CODE_TO_STRING,
    AlphaDiaConstants,
)
from common.keys import (
    QUANTING_TIME_ELAPSED_METRIC,
    AirflowVars,
    CustomAlphaDiaStates,
    DagContext,
    DagParams,
    JobStates,
    OpArgs,
    QuantingEnv,
    XComKeys,
)
from common.paths import (
    get_fallback_project_id,
    get_internal_output_path_for_raw_file,
    get_output_folder_rel_path,
)
from common.utils import (
    get_airflow_variable,
    get_env_variable,
    get_xcom,
    put_xcom,
)
from metrics.metrics_calculator import calc_metrics
from sensors.ssh_sensor import SSHSensorOperator

from shared.db.interface import (
    add_metrics_to_raw_file,
    get_raw_file_by_id,
    get_settings_for_project,
    update_raw_file,
)
from shared.db.models import RawFile, RawFileStatus, get_created_at_year_month
from shared.keys import EnvVars


def _get_project_id_or_fallback(project_id: str | None, instrument_id: str) -> str:
    """Get the project id for a raw file or the fallback ID if not present."""
    return (
        project_id if project_id is not None else get_fallback_project_id(instrument_id)
    )


def prepare_quanting(ti: TaskInstance, **kwargs) -> None:
    """Prepare the environmental variables for the quanting job."""
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]

    raw_file = get_raw_file_by_id(raw_file_id)
    pool_base_path = Path(get_env_variable(EnvVars.POOL_BASE_PATH))

    # get raw_file_path
    backup_pool_folder = get_env_variable(EnvVars.BACKUP_POOL_FOLDER)
    year_month_subfolder = get_created_at_year_month(raw_file)
    raw_file_path = (
        pool_base_path
        / backup_pool_folder
        / instrument_id
        / year_month_subfolder
        / raw_file_id
    )

    # get settings and output_path
    quanting_pool_folder = get_env_variable(EnvVars.QUANTING_POOL_FOLDER)
    project_id_or_fallback = _get_project_id_or_fallback(
        raw_file.project_id, instrument_id
    )

    settings_path = (
        pool_base_path / quanting_pool_folder / "settings" / project_id_or_fallback
    )

    output_path = (
        pool_base_path
        / quanting_pool_folder
        / get_output_folder_rel_path(raw_file, project_id_or_fallback)
    )

    settings = get_settings_for_project(project_id_or_fallback)

    quanting_env = {
        QuantingEnv.RAW_FILE_PATH: str(raw_file_path),
        QuantingEnv.SETTINGS_PATH: str(settings_path),
        QuantingEnv.OUTPUT_PATH: str(output_path),
        QuantingEnv.SPECLIB_FILE_NAME: settings.speclib_file_name,
        QuantingEnv.FASTA_FILE_NAME: settings.fasta_file_name,
        QuantingEnv.CONFIG_FILE_NAME: settings.config_file_name,
        QuantingEnv.SOFTWARE: settings.software,
        # not required for slurm script:
        QuantingEnv.RAW_FILE_ID: raw_file_id,
        QuantingEnv.PROJECT_ID_OR_FALLBACK: project_id_or_fallback,
        QuantingEnv.SETTINGS_VERSION: settings.version,
    }

    put_xcom(ti, XComKeys.QUANTING_ENV, quanting_env)
    # this is redundant to the entry in QUANTING_ENV, but makes downstream access a bit more convenient
    put_xcom(ti, XComKeys.RAW_FILE_ID, raw_file_id)


def _create_export_command(mapping: dict[str, str]) -> str:
    """Create a bash command to export environment variables."""
    return "\n".join([f"export {k}={v}" for k, v in mapping.items()])


def run_quanting(ti: TaskInstance, **kwargs) -> None:
    """Run the quanting job on the cluster."""
    del kwargs  # unused
    # IMPLEMENT:
    # wait for the cluster to be ready (20% idling) -> dedicated (sensor) task, mind race condition! (have pool = 1 for that)

    quanting_env = get_xcom(ti, XComKeys.QUANTING_ENV)

    # upfront check 1
    if (job_id := get_xcom(ti, XComKeys.JOB_ID, -1)) != -1:
        logging.warning(f"Job already started with {job_id}, skipping.")
        return

    raw_file = get_raw_file_by_id(quanting_env[QuantingEnv.RAW_FILE_ID])

    # upfront check 2
    output_path = get_internal_output_path_for_raw_file(
        raw_file,
        project_id_or_fallback=quanting_env[QuantingEnv.PROJECT_ID_OR_FALLBACK],
    )
    if Path(output_path).exists():
        msg = f"Output path {output_path} already exists."
        if get_airflow_variable(AirflowVars.ALLOW_OUTPUT_OVERWRITE, "False") != "True":
            raise AirflowFailException(
                f"{msg} Remove it before restarting the quanting or set ALLOW_OUTPUT_OVERWRITE."
            )
        logging.warning(f"{msg} Overwriting it because ALLOW_OUTPUT_OVERWRITE is set.")

    year_month_folder = get_created_at_year_month(raw_file)

    command = _create_export_command(quanting_env) + get_run_quanting_cmd(
        year_month_folder
    )
    logging.info(f"Running command: >>>>\n{command}\n<<<< end of command")

    ssh_return = SSHSensorOperator.ssh_execute(command)

    try:
        job_id = str(int(ssh_return.split("\n")[-1]))
    except Exception as e:
        logging.exception("Did not get a valid job id from the cluster.")
        raise AirflowFailException("Job submission failed.") from e

    update_raw_file(
        quanting_env[QuantingEnv.RAW_FILE_ID], new_status=RawFileStatus.QUANTING
    )

    put_xcom(ti, XComKeys.JOB_ID, job_id)


def _get_custom_error_codes(events_jsonl_file_path: Path) -> list[str]:
    """Extract the error codes from the events.jsonl file."""
    error_codes = []
    with events_jsonl_file_path.open() as file:
        for line in file:
            try:
                data = json.loads(line.strip())
                if data.get("name") == "exception" and data.get("error_code", "") != "":
                    error_codes.append(data["error_code"])
            except json.JSONDecodeError:  # noqa: PERF203
                logging.warning(f"Skipping invalid JSON: {line.strip()}")
    return error_codes


def _get_other_error_codes(output_path: Path) -> str:
    """Extract non-custom errors from the alphaDIA logs."""
    log_file_path = output_path / AlphaDiaConstants.LOG_FILE_NAME
    if not log_file_path.exists():
        logging.warning(f"Could not find {log_file_path=}")
        return CustomAlphaDiaStates.NO_LOG_FILE

    with log_file_path.open() as file:
        for line in reversed(file.readlines()):
            if "ERROR" in line:
                logging.info(f"Found error line: {line.strip()}")
                for error_code, error_string in ERROR_CODE_TO_STRING.items():
                    if error_string in line:
                        return error_code
                return CustomAlphaDiaStates.UNKNOWN_ERROR
    return CustomAlphaDiaStates.COULD_NOT_DETERMINE_ERROR


def get_business_errors(raw_file: RawFile, project_id: str) -> list[str]:
    """Extract business errors from the alphaDIA output."""
    output_path = get_internal_output_path_for_raw_file(raw_file, project_id)

    raw_file_progress_subfolder = Path(raw_file.id).stem
    events_jsonl_path = (
        output_path
        / AlphaDiaConstants.PROGRESS_FOLDER_NAME
        / raw_file_progress_subfolder
        / AlphaDiaConstants.EVENTS_FILE_NAME
    )

    error_codes = []
    try:
        error_codes = _get_custom_error_codes(events_jsonl_path)
    except FileNotFoundError:
        logging.warning(f"Could not find {events_jsonl_path=}")

    if not error_codes:
        error_codes.append(_get_other_error_codes(output_path))

    return error_codes


def check_quanting_result(ti: TaskInstance, **kwargs) -> bool:
    """Get info (slurm log, alphaDIA log) about a job from the cluster.

    Return False in case downstream tasks should be skipped, True otherwise.
    """
    del kwargs  # unused
    job_id = get_xcom(ti, XComKeys.JOB_ID)

    # the wildcard here is a bit of a hack to avoid retrieving the year_month
    # subfolder here .. should be no problem if job_ids are unique
    slurm_output_file = f"{CLUSTER_WORKING_DIR}/*/slurm-{job_id}.out"
    cmd = check_quanting_result_cmd(job_id, slurm_output_file) + get_job_state_cmd(
        job_id
    )
    ssh_return = SSHSensorOperator.ssh_execute(cmd)

    time_elapsed = _get_time_elapsed(ssh_return)
    put_xcom(ti, XComKeys.QUANTING_TIME_ELAPSED, time_elapsed)

    job_status = ssh_return.split("\n")[-1]
    logging.info(f"Job {job_id} exited with status {job_status}.")

    if job_status == JobStates.COMPLETED:
        return True  # continue with downstream tasks

    if job_status in [JobStates.FAILED, JobStates.TIMEOUT]:
        quanting_env = get_xcom(ti, XComKeys.QUANTING_ENV)
        project_id = quanting_env[QuantingEnv.PROJECT_ID_OR_FALLBACK]
        raw_file = get_raw_file_by_id(quanting_env[QuantingEnv.RAW_FILE_ID])

        if job_status == JobStates.FAILED:
            errors = get_business_errors(raw_file, project_id)
        else:
            errors = ["TIMEOUT"]

        update_raw_file(
            raw_file.id,
            new_status=RawFileStatus.QUANTING_FAILED,
            status_details=";".join(errors),
        )
        add_metrics_to_raw_file(
            raw_file.id,
            metrics={QUANTING_TIME_ELAPSED_METRIC: time_elapsed},
            settings_version=quanting_env[QuantingEnv.SETTINGS_VERSION],
        )

        # fail the DAG without retry on new errors to make them transparent in Airflow UI
        states_to_fail_task = [
            CustomAlphaDiaStates.UNKNOWN_ERROR,
            CustomAlphaDiaStates.NO_LOG_FILE,
            CustomAlphaDiaStates.UNKNOWN_ERROR,
        ]
        if any(state in errors for state in states_to_fail_task):
            raise AirflowFailException(f"Quanting failed with new error: {errors=}")

        return False  # skip downstream tasks

    # unknown state: fail the DAG without retry
    logging.info(f"Job {job_id} exited with status {job_status}.")
    raise AirflowFailException(f"Quanting failed: {job_status=}")


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

    raw_file = get_raw_file_by_id(quanting_env[QuantingEnv.RAW_FILE_ID])

    output_path = get_internal_output_path_for_raw_file(
        raw_file, quanting_env[QuantingEnv.PROJECT_ID_OR_FALLBACK]
    )
    metrics = calc_metrics(output_path)

    put_xcom(ti, XComKeys.METRICS, metrics)


def upload_metrics(ti: TaskInstance, **kwargs) -> None:
    """Upload the metrics to the database."""
    del kwargs

    raw_file_id = get_xcom(ti, XComKeys.RAW_FILE_ID)
    quanting_env = get_xcom(ti, XComKeys.QUANTING_ENV)
    metrics: dict = get_xcom(
        ti, XComKeys.METRICS
    )  # pytype: disable=annotation-type-mismatch

    metrics[QUANTING_TIME_ELAPSED_METRIC] = get_xcom(ti, XComKeys.QUANTING_TIME_ELAPSED)

    add_metrics_to_raw_file(
        raw_file_id,
        metrics=metrics,
        settings_version=quanting_env[QuantingEnv.SETTINGS_VERSION],
    )

    update_raw_file(raw_file_id, new_status=RawFileStatus.DONE)
