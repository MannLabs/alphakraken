"""Business logic for the acquisition_processor."""

import json
import logging
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance
from common.constants import (
    DEFAULT_JOB_SCRIPT_NAME,
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
from shared.db.models import RawFile, RawFileStatus, Settings, get_created_at_year_month
from shared.keys import MetricsTypes, SoftwareTypes
from shared.validation import validate_config_params, validate_name
from shared.yamlsettings import YamlKeys, get_path


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
    backup_base_path = get_path(YamlKeys.Locations.BACKUP)
    year_month_subfolder = get_created_at_year_month(raw_file)
    raw_file_path = (
        backup_base_path / instrument_id / year_month_subfolder / raw_file_id
    )

    # get settings and output_path
    settings_path = get_path(YamlKeys.Locations.SETTINGS) / project_id_or_fallback

    output_path = get_path(YamlKeys.Locations.OUTPUT) / get_output_folder_rel_path(
        raw_file, project_id_or_fallback
    )

    if settings.software_type == SoftwareTypes.CUSTOM:
        custom_command = _prepare_custom_command(
            output_path, raw_file_path, settings, settings_path
        )
    else:
        custom_command = ""

    quanting_env = {
        QuantingEnv.RAW_FILE_PATH: str(raw_file_path),
        QuantingEnv.SETTINGS_PATH: str(settings_path),
        QuantingEnv.OUTPUT_PATH: str(output_path),
        QuantingEnv.SPECLIB_FILE_NAME: settings.speclib_file_name,  # TODO: construct path here
        QuantingEnv.FASTA_FILE_NAME: settings.fasta_file_name,  # TODO: construct path here
        QuantingEnv.CONFIG_FILE_NAME: settings.config_file_name,  # TODO: construct path here
        QuantingEnv.CONFIG_PARAMS: settings.config_params,
        QuantingEnv.SOFTWARE: settings.software,
        QuantingEnv.SOFTWARE_TYPE: settings.software_type,
        QuantingEnv.CUSTOM_COMMAND: custom_command,
        # not required for slurm script:
        QuantingEnv.RAW_FILE_ID: raw_file_id,
        QuantingEnv.PROJECT_ID_OR_FALLBACK: project_id_or_fallback,
        QuantingEnv.SETTINGS_VERSION: settings.version,
    }

    errors = []
    for to_validate in quanting_env.values():
        if to_validate and isinstance(to_validate, str):
            errors.extend(validate_name(to_validate))
    errors.extend(validate_config_params(settings.config_params))
    if errors:
        raise AirflowFailException(
            f"Validation errors in quanting environment: {errors}"
        )

    put_xcom(ti, XComKeys.QUANTING_ENV, quanting_env)
    # this is redundant to the entry in QUANTING_ENV, but makes downstream access a bit more convenient
    put_xcom(ti, XComKeys.RAW_FILE_ID, raw_file_id)


def _prepare_custom_command(
    output_path: Path, raw_file_path: Path, settings: Settings, settings_path: Path
) -> str:
    """Prepare the custom command for the quanting job."""
    speclib_file_path = (
        str(settings_path / settings.speclib_file_name)
        if settings.speclib_file_name
        else ""
    )
    fasta_file_path = (
        str(settings_path / settings.fasta_file_name)
        if settings.fasta_file_name
        else ""
    )
    substituted_params = settings.config_params
    substituted_params = substituted_params.replace("RAW_FILE_PATH", str(raw_file_path))
    substituted_params = substituted_params.replace("LIB_PATH", speclib_file_path)
    substituted_params = substituted_params.replace("OUTPUT_PATH", str(output_path))
    substituted_params = substituted_params.replace("FASTA_PATH", fasta_file_path)

    # TODO: fail here if RAW_FILE_PATH, OUT_PATH are not replaced, and if fasta_file_path,speclib_file_path are given but not replaced, also in frontend
    # TODO: fail here if something looks wrong with the command, also in frontend
    software_base_path = get_path(YamlKeys.Locations.SOFTWARE)
    software_path = str(software_base_path / settings.software)
    custom_command = f"{software_path} {substituted_params}"
    logging.info(f"Custom command for quanting: {custom_command}")
    return custom_command


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


def run_quanting(
    ti: TaskInstance,
    *,
    new_status: str | None = RawFileStatus.QUANTING,
    output_path_check: bool = True,
    job_script_name: str = DEFAULT_JOB_SCRIPT_NAME,
    xcom_key_job_id: str = XComKeys.JOB_ID,
    **kwargs,
) -> None:
    """Run a job on the cluster.

    Use functools.partial to pass additional arguments to this task.

    :param ti: TaskInstance object to interact with Airflow's XCom.
    :param new_status: The status to set for the raw file after starting the job, default is RawFileStatus.QUANTING'.
        Set to None to not change the status.
    :param output_path_check: Whether to check if the output path already exists
        and handle it accordingly, default is True. Setting to false will overwrite contents of the output path.
    :param job_script_name: The name of the job script to run, default is DEFAULT_JOB_SCRIPT_NAME.
    :param xcom_key_job_id: The key to use for storing the job ID in XCom, default is XComKeys.JOB_ID.
    """
    del kwargs  # unused

    quanting_env = get_xcom(ti, XComKeys.QUANTING_ENV)

    # upfront check 1
    if (job_id := get_xcom(ti, xcom_key_job_id, -1)) != -1:
        logging.warning(f"Job already started with {job_id}, skipping.")
        return

    raw_file = get_raw_file_by_id(quanting_env[QuantingEnv.RAW_FILE_ID])

    # upfront check 2
    output_path = get_internal_output_path_for_raw_file(
        raw_file,
        project_id_or_fallback=quanting_env[QuantingEnv.PROJECT_ID_OR_FALLBACK],
    )
    if output_path_check and Path(output_path).exists():
        msg = f"Output path {output_path} already exists with different content."
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

            put_xcom(ti, xcom_key_job_id, extracted_job_id)

            logging.warning(f"Assuming job id {extracted_job_id}...")
            return
        else:
            raise AirflowFailException(
                f"{msg} Remove it before restarting the quanting or set Airflow variable 'output_exists_mode' to 'overwrite' or 'associate' "
                f"(got {output_exists_mode})"
            )

    year_month_folder = get_created_at_year_month(raw_file)

    job_id = start_job(job_script_name, quanting_env, year_month_folder)

    if new_status is not None:
        update_raw_file(quanting_env[QuantingEnv.RAW_FILE_ID], new_status=new_status)

    put_xcom(ti, xcom_key_job_id, job_id)


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

    if job_status in [JobStates.FAILED, JobStates.TIMEOUT] or job_status.startswith(
        JobStates.OUT_OF_MEMORY
    ):
        quanting_env = get_xcom(ti, XComKeys.QUANTING_ENV)
        project_id = quanting_env[QuantingEnv.PROJECT_ID_OR_FALLBACK]
        raw_file = get_raw_file_by_id(quanting_env[QuantingEnv.RAW_FILE_ID])

        if job_status == JobStates.FAILED:
            errors = get_business_errors(raw_file, project_id)
        elif job_status == JobStates.TIMEOUT:
            errors = ["TIMEOUT"]
        else:
            errors = ["OUT_OF_MEMORY"]

        update_raw_file(
            raw_file.id,
            new_status=RawFileStatus.QUANTING_FAILED,
            status_details=";".join(errors),
        )
        add_metrics_to_raw_file(
            raw_file.id,
            metrics={QUANTING_TIME_ELAPSED_METRIC: time_elapsed},
            settings_version=quanting_env[QuantingEnv.SETTINGS_VERSION],
            metrics_type=MetricsTypes.ALPHADIA,
        )

        # fail the DAG without retry on new errors to make them transparent in Airflow UI
        states_to_fail_task = [
            CustomAlphaDiaStates.UNKNOWN_ERROR,
            CustomAlphaDiaStates.NO_LOG_FILE,
            CustomAlphaDiaStates.COULD_NOT_DETERMINE_ERROR,
        ]
        if any(state in errors for state in states_to_fail_task):
            raise AirflowFailException(f"Quanting failed with new error: {errors=}")

        return False  # skip downstream tasks

    # unknown state: fail the DAG without retry
    logging.info(f"Job {job_id} exited with status {job_status}.")
    raise AirflowFailException(f"Quanting failed: {job_status=}")


def compute_metrics(
    ti: TaskInstance, *, metrics_type: str = MetricsTypes.ALPHADIA, **kwargs
) -> None:
    """Compute metrics from the quanting results."""
    del kwargs

    quanting_env = get_xcom(ti, XComKeys.QUANTING_ENV)

    raw_file = get_raw_file_by_id(quanting_env[QuantingEnv.RAW_FILE_ID])

    output_path = get_internal_output_path_for_raw_file(
        raw_file, quanting_env[QuantingEnv.PROJECT_ID_OR_FALLBACK]
    )

    metrics = calc_metrics(output_path, metrics_type=metrics_type)

    if metrics_type == MetricsTypes.ALPHADIA:
        # TODO: the time measurement also needs to be generified
        metrics[QUANTING_TIME_ELAPSED_METRIC] = get_xcom(
            ti, XComKeys.QUANTING_TIME_ELAPSED
        )

    put_xcom(ti, XComKeys.METRICS, metrics)
    put_xcom(ti, XComKeys.METRICS_TYPE, metrics_type)


def upload_metrics(ti: TaskInstance, **kwargs) -> None:
    """Upload the metrics to the database."""
    del kwargs

    raw_file_id = get_xcom(ti, XComKeys.RAW_FILE_ID)
    quanting_env = get_xcom(ti, XComKeys.QUANTING_ENV)
    metrics: dict = get_xcom(ti, XComKeys.METRICS)
    metrics_type = get_xcom(ti, XComKeys.METRICS_TYPE)

    add_metrics_to_raw_file(
        raw_file_id,
        metrics_type=metrics_type,
        metrics=metrics,
        settings_version=quanting_env[QuantingEnv.SETTINGS_VERSION],
    )

    update_raw_file(
        raw_file_id, new_status=RawFileStatus.DONE, status_details=None
    )  # TODO: move to dedicated reusable status update task
