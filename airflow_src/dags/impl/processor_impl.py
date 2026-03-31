"""Business logic for the acquisition_processor."""

import json
import logging
from pathlib import Path

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import TaskInstance
from airflow.utils.state import TaskInstanceState
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
from common.settings import get_instrument_settings
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
    get_project_settings,
    get_raw_file_by_id,
    update_raw_file,
)
from shared.db.models import RawFile, RawFileStatus, Settings, get_created_at_year_month
from shared.keys import SoftwareTypes
from shared.scope import resolve_scoped_settings
from shared.validation import check_for_malicious_content
from shared.yamlsettings import YamlKeys, get_path


def prepare_quanting(raw_file_id: str) -> list[dict[str, str | int | list[str]]]:
    """Prepare the environmental variables for the quanting job."""
    raw_file = get_raw_file_by_id(raw_file_id)
    instrument_id = raw_file.instrument_id

    instrument_type = get_instrument_settings(instrument_id, InstrumentKeys.TYPE)
    try:
        project_settings = get_project_settings(raw_file.project_id)
        settings_list = resolve_scoped_settings(
            project_settings,
            instrument_id=instrument_id,
            instrument_type=instrument_type,
            raw_file_id=raw_file_id,
        )
    except DoesNotExist as e:
        # this should not happen as every project has a fallback setting
        raise AirflowFailException(
            f"Project or settings not found for '{raw_file.project_id}'."
        ) from e

    if not settings_list:
        # this should not happen as this DAG should not be triggered if there are no settings
        raise AirflowFailException(
            f"No settings assigned to project '{raw_file.project_id}'."
        )

    # Create the base output folder (once per raw file).
    # Settings-specific subfolders are created later
    base_output_path = get_internal_output_path_for_raw_file(raw_file)
    base_output_path.mkdir(parents=True, exist_ok=True)

    # get raw file path
    backup_base_path = get_path(YamlKeys.Locations.BACKUP)
    year_month_subfolder = get_created_at_year_month(raw_file)
    relative_raw_file_path = Path(instrument_id) / year_month_subfolder / raw_file_id
    raw_file_path = backup_base_path / relative_raw_file_path

    quanting_envs: list[dict[str, str | int | list[str]]] = []
    for settings in settings_list:
        quanting_env = _create_quanting_env(
            settings,
            raw_file,
            raw_file_path,
            relative_raw_file_path,
        )

        if errors := _check_content(quanting_env, settings):
            # this is a bit of a hack to propagate errors to the individual downstream branches
            quanting_env[QuantingEnv.QUANTING_ENV_CREATION_ERRORS] = errors
            logging.warning(f"quanting_env for {settings.name} has errors: {errors}")

        quanting_envs.append(quanting_env)

    return quanting_envs


def _create_quanting_env(
    settings: Settings,
    raw_file: RawFile,
    raw_file_path: Path,
    relative_raw_file_path: Path,
) -> dict[str, str | int | list[str]]:
    """Create a quanting environment from settings."""
    settings_path = get_path(YamlKeys.Locations.SETTINGS) / settings.name

    relative_output_path = get_output_folder_rel_path(
        raw_file, settings_type=settings.software_type
    )
    output_path = get_path(YamlKeys.Locations.OUTPUT) / relative_output_path

    internal_output_path = get_internal_output_path_for_raw_file(
        raw_file, settings_type=settings.software_type
    )

    custom_command = (
        _prepare_custom_command(
            raw_file.id,
            relative_output_path,
            output_path,
            relative_raw_file_path,
            raw_file_path,
            settings,
            settings_path,
            settings.num_threads,
            raw_file.project_id,
        )
        # MSQC and skyline are treated as a 'custom command'
        if settings.software_type
        in [SoftwareTypes.CUSTOM, SoftwareTypes.SKYLINE, SoftwareTypes.MSQC]
        else ""
    )

    quanting_env: dict[str, str | int | list[str]] = {
        QuantingEnv.RAW_FILE_PATH: str(raw_file_path),
        QuantingEnv.SETTINGS_PATH: str(settings_path),
        QuantingEnv.OUTPUT_PATH: str(output_path),
        QuantingEnv.RELATIVE_OUTPUT_PATH: str(relative_output_path),
        QuantingEnv.SPECLIB_FILE_NAME: settings.speclib_file_name,  # TODO: construct path here
        QuantingEnv.FASTA_FILE_NAME: settings.fasta_file_name,  # TODO: construct path here
        QuantingEnv.CONFIG_FILE_NAME: settings.config_file_name,  # TODO: construct path here
        QuantingEnv.SOFTWARE: settings.software,
        QuantingEnv.SOFTWARE_TYPE: settings.software_type,
        QuantingEnv.METRICS_TYPE: settings.metrics_type,
        QuantingEnv.CUSTOM_COMMAND: custom_command,
        # job parameters
        QuantingEnv.SLURM_CPUS_PER_TASK: settings.slurm_cpus_per_task,
        QuantingEnv.SLURM_MEM: settings.slurm_mem,
        QuantingEnv.SLURM_TIME: settings.slurm_time,
        QuantingEnv.NUM_THREADS: settings.num_threads,
        # not required for slurm script:
        QuantingEnv.RAW_FILE_ID: raw_file.id,
        QuantingEnv.PROJECT_ID_OR_FALLBACK: raw_file.project_id,
        QuantingEnv.SETTINGS_NAME: settings.name,
        QuantingEnv.SETTINGS_VERSION: settings.version,
        QuantingEnv.INTERNAL_OUTPUT_PATH: str(internal_output_path),
    }
    return quanting_env


def _prepare_custom_command(  # noqa: PLR0913 Too many arguments
    raw_file_id: str,
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
    if settings.config_params is None:
        substituted_params = ""
    else:
        substituted_params = settings.config_params
        replacements = {
            # mind the order of replacements here (LONGER placeholders first, e.g. RAW_FILE_PATH before RELATIVE_RAW_FILE_PATH)
            "RELATIVE_RAW_FILE_PATH": relative_raw_file_path,
            "RAW_FILE_PATH": raw_file_path,
            "RAW_FILE_ID": raw_file_id,
            "SETTINGS_PATH": settings_path,
            "RELATIVE_OUTPUT_PATH": relative_output_path,
            "OUTPUT_PATH": output_path,
            "NUM_THREADS": num_threads,
            "PROJECT_ID": project_id,
        }
        for placeholder, new_value in replacements.items():
            substituted_params = substituted_params.replace(placeholder, str(new_value))

    software_base_path = get_path(YamlKeys.Locations.SOFTWARE)
    software_path = str(software_base_path / settings.software)

    custom_command = f"{software_path} {substituted_params}"
    logging.info(f"Custom command for quanting: {custom_command}")
    return custom_command


def _check_content(
    quanting_env: dict[str, str | int | list[str]], settings: Settings
) -> list[str]:
    """Validate the fields in the quanting environment don't contain malicious content."""
    absolute_path_allowed_keys = [
        QuantingEnv.RAW_FILE_PATH,
        QuantingEnv.SETTINGS_PATH,
        QuantingEnv.OUTPUT_PATH,
        QuantingEnv.INTERNAL_OUTPUT_PATH,
        QuantingEnv.SOFTWARE,
    ]

    errors = []
    for key, value in quanting_env.items():
        if (
            value
            and key
            not in [
                QuantingEnv.CUSTOM_COMMAND,  # validated below
                QuantingEnv.SLURM_TIME,  # contains ":", validated in webapp
            ]
            and isinstance(value, str)
            and (
                errors_ := check_for_malicious_content(
                    value, allow_absolute_paths=key in absolute_path_allowed_keys
                )
            )
        ):
            errors.append(f"Validation error in '{value}': {errors_}")

    if quanting_env.get(QuantingEnv.CUSTOM_COMMAND):
        errors.extend(
            check_for_malicious_content(
                str(quanting_env[QuantingEnv.CUSTOM_COMMAND]),
                allow_spaces=True,
                allow_absolute_paths=True,
            )
        )
    if settings.config_params:
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
    # TODO: revisit/remove those 3 parameters
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
    logging.info(f"Starting quanting with environment: {quanting_env}")

    if quanting_env.get(QuantingEnv.QUANTING_ENV_CREATION_ERRORS):
        raise AirflowFailException(
            f"Quanting environment construction failed:\n {quanting_env}"
        )

    raw_file = get_raw_file_by_id(quanting_env[QuantingEnv.RAW_FILE_ID])

    if get_instrument_settings(raw_file.instrument_id, InstrumentKeys.SKIP_QUANTING):
        logging.info(
            f"Skipping quanting for raw file {raw_file.id} because instrument settings have skip_quanting=True."
        )
        raise AirflowSkipException("Skipping quanting due to instrument settings.")

    # upfront check 2
    output_path = Path(quanting_env[QuantingEnv.INTERNAL_OUTPUT_PATH])
    if output_path_check and output_path.exists():
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

    output_path.mkdir(parents=True, exist_ok=True)

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


def get_business_errors(raw_file: RawFile, output_path: Path) -> list[str]:
    """Extract business errors from the alphaDIA output."""
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


def check_quanting_result(*, quanting_env: dict, job_id: str, ti: TaskInstance) -> dict:
    """Get info (slurm log, alphaDIA log) about a job from the cluster.

    :param quanting_env: The quanting environment variables dict.
    :param job_id: The Slurm job ID to check.
    :param ti: The Airflow TaskInstance, used to push error details to XCom.
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
        raw_file = get_raw_file_by_id(quanting_env[QuantingEnv.RAW_FILE_ID])
        output_path = Path(quanting_env[QuantingEnv.INTERNAL_OUTPUT_PATH])

        if job_status == JobStates.FAILED:
            if quanting_env[QuantingEnv.SOFTWARE_TYPE] == SoftwareTypes.ALPHADIA:
                errors = get_business_errors(raw_file, output_path)
            else:
                errors = ["FAILED"]
        elif job_status == JobStates.TIMEOUT:
            errors = ["TIMEOUT"]
        else:
            # TODO: this seems not quite right
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
            ti.xcom_push(key=BRANCH_ERRORS_XCOM_KEY, value=";".join(errors))
            raise AirflowFailException(f"Quanting failed with new error: {errors=}")

        ti.xcom_push(key=BRANCH_ERRORS_XCOM_KEY, value=";".join(errors))
        raise AirflowSkipException("Job failed, skipping downstream tasks.")

    # unknown state: fail the DAG without retry
    ti.xcom_push(key=BRANCH_ERRORS_XCOM_KEY, value=f"unknown_job_status:{job_status}")
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
    metrics_type = quanting_env[QuantingEnv.METRICS_TYPE]
    output_path = Path(quanting_env[QuantingEnv.INTERNAL_OUTPUT_PATH])

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


BRANCH_ERRORS_XCOM_KEY = "branch_errors"
MAX_STATUS_DETAILS_LENGTH = 512

FAILED_STATES = {TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED}
_UPLOAD_METRICS_TASK_ID = f"{TaskGroups.QUANTING_PIPELINE}.{Tasks.UPLOAD_METRICS}"
_CHECK_RESULT_TASK_ID = f"{TaskGroups.QUANTING_PIPELINE}.{Tasks.CHECK_QUANTING_RESULT}"


def finalize_raw_file_status(ti: TaskInstance, raw_file_id: str) -> None:
    """Set the final status for the raw file based on all pipeline branch outcomes.

    Inspects all parallel branch outcomes and sets the final status:
    - DONE if all branches succeeded
    - QUANTING_FAILED if some branches had known business errors (no Airflow failures)
    - ERROR if any branch had an Airflow failure
    """
    dag_run = ti.get_dagrun()
    tis = dag_run.get_task_instances()

    upload_tis = [t for t in tis if t.task_id == _UPLOAD_METRICS_TASK_ID]

    if not upload_tis:
        raise AirflowFailException("No upload_metrics task instances found in DAG run.")

    quanting_envs = ti.xcom_pull(task_ids=Tasks.PREPARE_QUANTING)

    airflow_errors: list[tuple[str, str]] = []
    business_errors: list[tuple[str, str]] = []

    for upload_ti in upload_tis:
        if upload_ti.state == TaskInstanceState.SUCCESS:
            continue

        idx = upload_ti.map_index
        settings_name = quanting_envs[idx][QuantingEnv.SETTINGS_NAME]
        branch_error_details = ti.xcom_pull(
            task_ids=_CHECK_RESULT_TASK_ID, map_indexes=idx, key=BRANCH_ERRORS_XCOM_KEY
        )

        if upload_ti.state in FAILED_STATES:
            airflow_errors.append(
                (settings_name, branch_error_details or "pipeline error")
            )
        elif upload_ti.state == TaskInstanceState.SKIPPED and branch_error_details:
            business_errors.append((settings_name, branch_error_details))

    if airflow_errors:
        all_errors = airflow_errors + business_errors
        details = _build_status_details(all_errors)
        logging.info(
            f"{len(airflow_errors)} branch(es) failed for {raw_file_id}: {details}"
        )
        update_raw_file(
            raw_file_id, new_status=RawFileStatus.ERROR, status_details=details
        )
        raise AirflowFailException(f"Pipeline error for {raw_file_id}: {details}")

    if business_errors:
        details = _build_status_details(business_errors)
        logging.info(
            f"{len(business_errors)} branch(es) with business errors for {raw_file_id}: {details}"
        )
        update_raw_file(
            raw_file_id,
            new_status=RawFileStatus.QUANTING_FAILED,
            status_details=details,
        )
        return

    update_raw_file(raw_file_id, new_status=RawFileStatus.DONE, status_details=None)


def _build_status_details(errors: list[tuple[str, str]]) -> str:
    """Join per-branch error tuples into a single status_details string, truncating if needed."""
    details = "; ".join(f"{name}: {err}" for name, err in errors)
    if len(details) > MAX_STATUS_DETAILS_LENGTH:
        details = details[: MAX_STATUS_DETAILS_LENGTH - 3] + "..."
    return details
