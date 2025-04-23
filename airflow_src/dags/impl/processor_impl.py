"""Business logic for the acquisition_processor."""

import json
import logging
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance
from common.constants import (
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
    get_internal_output_path_for_raw_file,
    get_output_folder_rel_path,
)
from common.settings import get_fallback_project_id
from common.utils import (
    get_airflow_variable,
    get_env_variable,
    get_xcom,
    put_xcom,
)
from jobs.job_handler import (
    get_job_result,
    start_job,
)
from metrics.metrics_calculator import calc_metrics
from mongoengine import DoesNotExist

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

    project_id_or_fallback = _get_project_id_or_fallback(
        raw_file.project_id, instrument_id
    )
    try:
        settings = get_settings_for_project(project_id_or_fallback)
    except DoesNotExist as e:
        raise AirflowFailException(
            f"No project found with id '{project_id_or_fallback}'. Please add a project and settings in the WebApp."
        ) from e
    else:
        if settings is None:
            raise AirflowFailException(
                f"No active settings found for project id '{project_id_or_fallback}'. Please add settings in the WebApp."
            )

    # get raw file path
    backup_pool_folder = Path(get_env_variable(EnvVars.BACKUP_POOL_FOLDER))
    year_month_subfolder = get_created_at_year_month(raw_file)
    raw_file_path = (
        backup_pool_folder / instrument_id / year_month_subfolder / raw_file_id
    )

    # get settings and output_path
    settings_path = (
        Path(get_env_variable(EnvVars.QUANTING_SETTINGS_PATH)) / project_id_or_fallback
    )

    output_path = Path(
        get_env_variable(EnvVars.QUANTING_OUTPUT_PATH)
    ) / get_output_folder_rel_path(raw_file, project_id_or_fallback)

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


def _get_slurm_job_id_from_log(output_path: Path) -> str | None:
    """Extract the Slurm job id from the log file, return None if file or id not existing."""
    log_file = output_path / AlphaDiaConstants.LOG_FILE_NAME
    if not log_file.exists():
        return None

    with log_file.open() as file:
        for line in file:
            if "SLURM_JOB_ID:" in line or "slurm_job_id:" in line:
                return str(int(line.split()[-1]))

    return None


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
        output_exists_mode = get_airflow_variable(
            AirflowVars.OUTPUT_EXISTS_MODE, "raise"
        )
        if output_exists_mode == "overwrite":
            logging.warning(
                f"{msg} Overwriting it because Airflow variable output_exists_mode='overwrite' is set."
            )
        elif output_exists_mode == "associate":
            logging.warning(f"{msg} Trying to associate job.")

            if (extracted_job_id := _get_slurm_job_id_from_log(output_path)) is None:
                logging.exception("Could not read off job id from log file.")
                raise AirflowFailException("Job submission failed.")

            put_xcom(ti, XComKeys.JOB_ID, extracted_job_id)

            logging.warning(f"Assuming job id {extracted_job_id}...")
            return
        else:
            raise AirflowFailException(
                f"{msg} Remove it before restarting the quanting or set Airflow variable 'output_exists_mode' to 'overwrite' or 'associate' "
                f"(got {output_exists_mode})"
            )

    year_month_folder = get_created_at_year_month(raw_file)

    job_id = start_job(quanting_env, year_month_folder)

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

    job_status, time_elapsed = get_job_result(job_id)

    put_xcom(ti, XComKeys.QUANTING_TIME_ELAPSED, time_elapsed)

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
