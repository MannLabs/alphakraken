"""Business logic for the acquisition_processor."""

import json
import logging
from pathlib import Path

from airflow.exceptions import AirflowFailException, AirflowSkipException
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
    InstrumentKeys,
    JobStates,
    QuantingEnv,
    TaskGroups,
    Tasks,
)
from common.paths import (
    get_internal_output_path_for_raw_file,
    get_output_folder_rel_path,
)
from common.settings import get_fallback_project_id, get_instrument_settings
from common.utils import (
    get_airflow_variable,
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
from shared.validation import check_for_malicious_content
from shared.yamlsettings import YamlKeys, get_path

# TODO: only temporarily until set via UI
SOFTWARE_TYPE_TO_METRICS_TYPE = {
    SoftwareTypes.ALPHADIA: MetricsTypes.ALPHADIA,
    SoftwareTypes.CUSTOM: MetricsTypes.CUSTOM,
    SoftwareTypes.MSQC: MetricsTypes.MSQC,
}


def _get_project_id_or_fallback(project_id: str | None, instrument_id: str) -> str:
    """Get the project id for a raw file or the fallback ID if not present."""
    return (
        project_id if project_id is not None else get_fallback_project_id(instrument_id)
    )


def prepare_quanting(
    raw_file_id: str, instrument_id: str
) -> list[dict[str, str | int]]:
    """Prepare the environmental variables for the quanting job."""
    # TODO: make these configurable
    cpus_per_task = 8
    num_threads = 8
    mem = "62G"
    time = "02:00:00"

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
                f"No settings assigned to project '{project_id_or_fallback}'. "
                "Please assign settings to this project in the WebApp."
            )

    # get raw file path
    backup_base_path = get_path(YamlKeys.Locations.BACKUP)
    year_month_subfolder = get_created_at_year_month(raw_file)
    relative_raw_file_path = Path(instrument_id) / year_month_subfolder / raw_file_id
    raw_file_path = backup_base_path / relative_raw_file_path

    # get settings and output_path
    settings_path = get_path(YamlKeys.Locations.SETTINGS) / settings.name

    relative_output_path = get_output_folder_rel_path(raw_file, project_id_or_fallback)
    output_path = get_path(YamlKeys.Locations.OUTPUT) / relative_output_path

    custom_command = (
        _prepare_custom_command(
            relative_output_path,
            output_path,
            relative_raw_file_path,
            raw_file_path,
            settings,
            settings_path,
            num_threads,
            project_id_or_fallback,
        )
        if settings.software_type == SoftwareTypes.CUSTOM
        else ""
    )

    quanting_env: dict[str, str | int] = {
        QuantingEnv.RAW_FILE_PATH: str(raw_file_path),
        QuantingEnv.SETTINGS_PATH: str(settings_path),
        QuantingEnv.OUTPUT_PATH: str(output_path),
        QuantingEnv.RELATIVE_OUTPUT_PATH: str(relative_output_path),
        QuantingEnv.SPECLIB_FILE_NAME: settings.speclib_file_name,  # TODO: construct path here
        QuantingEnv.FASTA_FILE_NAME: settings.fasta_file_name,  # TODO: construct path here
        QuantingEnv.CONFIG_FILE_NAME: settings.config_file_name,  # TODO: construct path here
        QuantingEnv.SOFTWARE: settings.software,
        QuantingEnv.SOFTWARE_TYPE: settings.software_type,
        QuantingEnv.METRICS_TYPE: SOFTWARE_TYPE_TO_METRICS_TYPE[settings.software_type],
        QuantingEnv.CUSTOM_COMMAND: custom_command,
        # job parameters
        QuantingEnv.SLURM_CPUS_PER_TASK: cpus_per_task,
        QuantingEnv.SLURM_MEM: mem,
        QuantingEnv.SLURM_TIME: time,
        QuantingEnv.NUM_THREADS: num_threads,
        # not required for slurm script:
        QuantingEnv.RAW_FILE_ID: raw_file_id,
        QuantingEnv.PROJECT_ID_OR_FALLBACK: project_id_or_fallback,
        QuantingEnv.SETTINGS_NAME: settings.name,
        QuantingEnv.SETTINGS_VERSION: settings.version,
    }

    if errors := _check_content(quanting_env, settings):
        raise AirflowFailException(
            f"Validation errors in quanting environment: {errors}"
        )

    return [quanting_env]


def _prepare_custom_command(  # noqa: PLR0913 Too many arguments
    relative_output_path: Path,
    output_path: Path,
    relative_raw_file_path: Path,
    raw_file_path: Path,
    settings: Settings,
    settings_path: Path,
    num_threads: int,
    project_id: str,
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

    replacements = {
        # mind the order of replacements here (LONGER placeholders first, e.g. RAW_FILE_PATH before RELATIVE_RAW_FILE_PATH)
        "RELATIVE_RAW_FILE_PATH": str(relative_raw_file_path),
        "RAW_FILE_PATH": str(raw_file_path),
        "LIBRARY_PATH": speclib_file_path,
        "RELATIVE_OUTPUT_PATH": str(relative_output_path),
        "OUTPUT_PATH": str(output_path),
        "FASTA_PATH": fasta_file_path,
        "NUM_THREADS": str(num_threads),
        "PROJECT_ID": project_id,
    }
    for placeholder, new_value in replacements.items():
        substituted_params = substituted_params.replace(placeholder, new_value)

    software_base_path = get_path(YamlKeys.Locations.SOFTWARE)
    software_path = str(software_base_path / settings.software)

    custom_command = f"{software_path} {substituted_params}"
    logging.info(f"Custom command for quanting: {custom_command}")
    return custom_command


def _check_content(quanting_env: dict[str, str | int], settings: Settings) -> list[str]:
    """Validate the fields in the quanting environment don't contain malicious content."""
    absolute_path_allowed_keys = [
        QuantingEnv.RAW_FILE_PATH,
        QuantingEnv.SETTINGS_PATH,
        QuantingEnv.OUTPUT_PATH,
        QuantingEnv.SOFTWARE,
    ]

    errors = []
    for key, value in quanting_env.items():
        if (
            value
            and key
            not in [
                QuantingEnv.CUSTOM_COMMAND,
                QuantingEnv.SLURM_TIME,  # TODO: contains ":", but set internally at the moment
            ]
            and isinstance(value, str)
            and (
                errors_ := check_for_malicious_content(
                    value, allow_absolute_paths=key in absolute_path_allowed_keys
                )
            )
        ):
            errors.append(f"Validation error in '{value}': {errors_}")

    if settings.software_type == SoftwareTypes.CUSTOM:
        errors.extend(
            check_for_malicious_content(
                str(quanting_env[QuantingEnv.CUSTOM_COMMAND]),
                allow_spaces=True,
                allow_absolute_paths=True,
            )
        )
        errors.extend(
            check_for_malicious_content(settings.config_params, allow_spaces=True)
        )

    return errors


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
    *,
    quanting_env: dict,
    new_status: str | None = RawFileStatus.QUANTING,
    output_path_check: bool = True,
    job_script_name: str = DEFAULT_JOB_SCRIPT_NAME,
) -> str:
    """Run a job on the cluster.

    :param quanting_env: The quanting environment variables dict.
    :param new_status: The status to set for the raw file after starting the job, default is RawFileStatus.QUANTING.
        Set to None to not change the status.
    :param output_path_check: Whether to check if the output path already exists
        and handle it accordingly, default is True. Setting to false will overwrite contents of the output path.
    :param job_script_name: The name of the job script to run, default is DEFAULT_JOB_SCRIPT_NAME.
    :return: The Slurm job ID as a string.
    """
    raw_file = get_raw_file_by_id(quanting_env[QuantingEnv.RAW_FILE_ID])

    if get_instrument_settings(raw_file.instrument_id, InstrumentKeys.SKIP_QUANTING):
        logging.info(
            f"Skipping quanting for raw file {raw_file.id} because instrument settings have skip_quanting=True."
        )
        raise AirflowSkipException("Skipping quanting due to instrument settings.")

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

            logging.warning(f"Assuming job id {extracted_job_id}...")
            return str(extracted_job_id)
        else:
            raise AirflowFailException(
                f"{msg} Remove it before restarting the quanting or set Airflow variable 'output_exists_mode' to 'overwrite' or 'associate' "
                f"(got {output_exists_mode})"
            )

    year_month_folder = get_created_at_year_month(raw_file)

    job_id = start_job(job_script_name, quanting_env, year_month_folder)

    if new_status is not None:
        update_raw_file(quanting_env[QuantingEnv.RAW_FILE_ID], new_status=new_status)

    return str(job_id)


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


def check_quanting_result(*, quanting_env: dict, job_id: str) -> dict:
    """Get info (slurm log, alphaDIA log) about a job from the cluster.

    :param quanting_env: The quanting environment variables dict.
    :param job_id: The Slurm job ID to check.
    :return: Dict with ``quanting_time_elapsed`` on success.
    :raises AirflowSkipException: On known job failures (skips downstream tasks).
    :raises AirflowFailException: On unknown job failures.
    """
    job_status, time_elapsed = get_job_result(job_id)

    logging.info(f"Job {job_id} exited with status {job_status}.")

    if job_status == JobStates.COMPLETED:
        return {"quanting_time_elapsed": time_elapsed}

    if job_status in [JobStates.FAILED, JobStates.TIMEOUT] or job_status.startswith(
        JobStates.OUT_OF_MEMORY
    ):
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
            settings_name=quanting_env[QuantingEnv.SETTINGS_NAME],
            settings_version=quanting_env[QuantingEnv.SETTINGS_VERSION],
            metrics_type=quanting_env[QuantingEnv.METRICS_TYPE],
        )

        # fail the DAG without retry on new errors to make them transparent in Airflow UI
        states_to_fail_task = [
            CustomAlphaDiaStates.UNKNOWN_ERROR,
            CustomAlphaDiaStates.NO_LOG_FILE,
            CustomAlphaDiaStates.COULD_NOT_DETERMINE_ERROR,
        ]
        if any(state in errors for state in states_to_fail_task):
            raise AirflowFailException(f"Quanting failed with new error: {errors=}")

        raise AirflowSkipException("Job failed, skipping downstream tasks.")

    # unknown state: fail the DAG without retry
    logging.info(f"Job {job_id} exited with status {job_status}.")
    raise AirflowFailException(f"Quanting failed: {job_status=}")


def compute_metrics(
    *,
    quanting_env: dict,
    quanting_time_elapsed: int | None = None,
) -> dict:
    """Compute metrics from the quanting results.

    :param quanting_env: The quanting environment variables dict.
    :param quanting_time_elapsed: Elapsed time from the quanting job, added to metrics if provided.
    :return: Dict with ``metrics`` and ``metrics_type``.
    """
    raw_file = get_raw_file_by_id(quanting_env[QuantingEnv.RAW_FILE_ID])
    metrics_type = quanting_env[QuantingEnv.METRICS_TYPE]

    output_path = get_internal_output_path_for_raw_file(
        raw_file, quanting_env[QuantingEnv.PROJECT_ID_OR_FALLBACK]
    )

    metrics = calc_metrics(output_path, metrics_type=metrics_type)

    if (
        quanting_time_elapsed is not None
    ):  # TODO: find a better way to handle this also for msqc
        metrics[QUANTING_TIME_ELAPSED_METRIC] = quanting_time_elapsed

    return {"metrics": metrics, "metrics_type": metrics_type}


def upload_metrics(*, quanting_env: dict, metrics: dict, metrics_type: str) -> None:
    """Upload the metrics to the database.

    :param quanting_env: The quanting environment variables dict.
    :param metrics: The computed metrics dictionary.
    :param metrics_type: The type of metrics (e.g. alphadia, custom).
    """
    raw_file_id = quanting_env[QuantingEnv.RAW_FILE_ID]

    add_metrics_to_raw_file(
        raw_file_id,
        metrics_type=metrics_type,
        metrics=metrics,
        settings_name=quanting_env[QuantingEnv.SETTINGS_NAME],
        settings_version=quanting_env[QuantingEnv.SETTINGS_VERSION],
    )


FAILED_STATES = {"failed", "upstream_failed"}
_UPLOAD_METRICS_TASK_ID = f"{TaskGroups.QUANTING_PIPELINE}.{Tasks.UPLOAD_METRICS}"


def finalize_raw_file_status(ti: TaskInstance, raw_file_id: str) -> None:
    """Set the final status for the raw file based on all pipeline branch outcomes."""
    dag_run = ti.get_dagrun()
    tis = dag_run.get_task_instances()

    upload_tis = [t for t in tis if t.task_id == _UPLOAD_METRICS_TASK_ID]

    if not upload_tis:
        raise AirflowFailException("No upload_metrics task instances found in DAG run.")

    failed = [t for t in upload_tis if t.state in FAILED_STATES]

    if failed:
        logging.info(
            f"{len(failed)} of {len(upload_tis)} branches failed for raw file {raw_file_id}."
        )
        update_raw_file(raw_file_id, new_status=RawFileStatus.ERROR)
        raise AirflowFailException("At least on task has failed.")
        # TODO: how to handle status_details? accumulate during the DAG run and reset here in case of success?

    # TODO: currently, if a task is skipped (due to a business error), the raw file will be marked as done.
    update_raw_file(raw_file_id, new_status=RawFileStatus.DONE, status_details=None)
