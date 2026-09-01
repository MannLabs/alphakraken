"""Business logic for the acquisition_processor."""

import json
import logging
from collections import defaultdict
from pathlib import Path, PurePosixPath

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.models import TaskInstance
from airflow.utils.state import TaskInstanceState
from common.constants import (
    ERROR_CODE_TO_STRING,
    AlphaDiaConstants,
)
from common.keys import (
    TIME_ELAPSED_METRIC,
    AirflowVars,
    CustomAlphaDiaStates,
    InstrumentKeys,
    JobStates,
    TaskGroups,
    Tasks,
    XComKeys,
)
from common.paths import (
    get_internal_output_path,
    get_internal_output_path_for_raw_file,
)
from common.quanting_env import QuantingEnv
from common.settings import get_instrument_settings
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

from shared.config_params import (
    ConfigParamPlaceholders,
    substitute_dummy_values,
    substitute_placeholders,
)
from shared.db.interface import (
    add_metrics_to_raw_file,
    get_project_settings,
    get_raw_file_by_id,
    get_settings_by_id,
    update_raw_file,
)
from shared.db.models import RawFile, RawFileStatus, Settings, get_created_at_year_month
from shared.keys import SoftwareTypes
from shared.path_layout import get_output_folder_rel_path, get_raw_file_rel_path
from shared.path_views import CLUSTER_VIEW, Locations
from shared.settings_scope_resolver import resolve_scoped_settings
from shared.validation import check_for_malicious_content


class QuantingFailedNewErrorException(AirflowFailException):
    """Raise if quanting failed with a new error."""


class QuantingFailedKnownErrorException(AirflowSkipException):
    """Raise if quanting failed with a known error."""


class QuantingFailedUnknownErrorException(AirflowFailException):
    """Raise if quanting failed with a unknown error state."""


class QuantingFailedException(AirflowFailException):
    """Raise if quanting failed but status has already been set."""


def resolve_settings(raw_file_id: str) -> list[str]:
    """Resolve which settings apply to a raw file."""
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

    return [str(s.id) for s in settings_list]  # type: ignore[unresolved-attribute]


def prepare_job(raw_file_id: str, settings_id: str) -> dict:
    """Prepare the environmental variables for the job.

    :return: The quanting environment as a dict, to be passed on via XCom.
    """
    raw_file = get_raw_file_by_id(raw_file_id)
    settings = get_settings_by_id(settings_id)

    relative_raw_file_path = get_raw_file_rel_path(raw_file)
    raw_file_path = CLUSTER_VIEW.resolve(Locations.BACKUP, relative_raw_file_path)

    internal_output_path = get_internal_output_path_for_raw_file(
        raw_file, software_type=settings.software_type
    )

    quanting_env = _create_quanting_env(
        settings,
        raw_file,
        raw_file_path,
        relative_raw_file_path,
        _get_output_path_suffix(internal_output_path),
    )

    if errors := _check_content(quanting_env, settings):
        raise AirflowFailException(
            f"Quanting env validation failed for '{settings.name}': {errors}"
        )

    return quanting_env.to_dict()


def _get_output_path_suffix(internal_output_path: Path) -> str:
    """Get the suffix to disambiguate an already existing output path, empty string if none is required."""
    if not internal_output_path.exists():
        return ""

    if get_airflow_variable(AirflowVars.OUTPUT_EXISTS_MODE, "raise") != "add":
        return ""

    suffix = _find_next_free_run_suffix(internal_output_path)
    logging.info(f"Output path {internal_output_path} exists. Using suffix '{suffix}'.")
    return suffix


def _find_next_free_run_suffix(base_path: Path) -> str:
    """Find the next free `.runN` suffix for the given base path."""
    run_number = 2
    while (base_path.parent / f"{base_path.name}.run{run_number}").exists():
        run_number += 1
    return f".run{run_number}"


def _create_quanting_env(
    settings: Settings,
    raw_file: RawFile,
    raw_file_path: PurePosixPath,
    relative_raw_file_path: Path,
    output_path_suffix: str = "",
) -> QuantingEnv:
    """Create a quanting environment from settings."""
    settings_path = CLUSTER_VIEW.resolve(Locations.SETTINGS, settings.name)

    relative_output_path = get_output_folder_rel_path(
        raw_file, software_type=settings.software_type
    )
    if output_path_suffix:
        relative_output_path = relative_output_path.with_name(
            relative_output_path.name + output_path_suffix
        )

    output_path = CLUSTER_VIEW.resolve(Locations.OUTPUT, relative_output_path)

    substituted_params = _substitute_config_params(
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

    custom_command = (
        _prepare_custom_command(settings, substituted_params)
        # all non-alphadia softwares are treated as 'custom command'
        if settings.software_type not in [SoftwareTypes.ALPHADIA]
        else ""
    )

    return QuantingEnv(
        raw_file_path=str(raw_file_path),
        settings_path=str(settings_path),
        output_path=str(output_path),
        relative_output_path=str(relative_output_path),
        speclib_file_name=settings.speclib_file_name,
        fasta_file_name=settings.fasta_file_name,
        config_file_name=settings.config_file_name,
        software=settings.software,
        software_type=settings.software_type,
        metrics_type=settings.metrics_type,
        custom_command=custom_command,
        # job parameters
        slurm_cpus_per_task=settings.slurm_cpus_per_task,
        slurm_mem=settings.slurm_mem,
        slurm_time=settings.slurm_time,
        num_threads=settings.num_threads,
        # not required for slurm script:
        raw_file_id=raw_file.id,
        project_id=raw_file.project_id,
        settings_name=settings.name,
        settings_version=settings.version,
        relative_raw_file_path=str(relative_raw_file_path),
        config_params=substituted_params,
        job_engine=settings.job_engine,
        year_month_folder=get_created_at_year_month(raw_file),
    )


def _substitute_config_params(  # noqa: PLR0913 Too many arguments
    raw_file_id: str,
    relative_output_path: Path,
    output_path: PurePosixPath,
    relative_raw_file_path: Path,
    raw_file_path: PurePosixPath,
    settings: Settings,
    settings_path: PurePosixPath,
    num_threads: int,
    project_id: str,
) -> str:
    """Resolve the placeholders in the configuration parameters of the settings."""
    if settings.config_params is None:
        return ""

    return substitute_placeholders(
        settings.config_params,
        {
            ConfigParamPlaceholders.PROJECT_ID: project_id,
            ConfigParamPlaceholders.RAW_FILE_ID: raw_file_id,
            ConfigParamPlaceholders.RAW_FILE_PATH: str(raw_file_path),
            ConfigParamPlaceholders.RELATIVE_RAW_FILE_PATH: str(relative_raw_file_path),
            ConfigParamPlaceholders.SETTINGS_PATH: str(settings_path),
            ConfigParamPlaceholders.OUTPUT_PATH: str(output_path),
            ConfigParamPlaceholders.RELATIVE_OUTPUT_PATH: str(relative_output_path),
            ConfigParamPlaceholders.NUM_THREADS: str(num_threads),
        },
    )


def _prepare_custom_command(settings: Settings, substituted_params: str) -> str:
    """Prepare the custom command for the quanting job."""
    software_path = str(CLUSTER_VIEW.resolve(Locations.SOFTWARE, settings.software))

    custom_command = f"{software_path} {substituted_params}"
    logging.info(f"Custom command for quanting: {custom_command}")
    return custom_command


def _check_content(quanting_env: QuantingEnv, settings: Settings) -> list[str]:
    """Validate the fields in the quanting environment don't contain malicious content."""
    absolute_path_allowed_fields = [
        "raw_file_path",
        "settings_path",
        "output_path",
        "software",
    ]
    # these hold resolved paths and are space-separated, so they need the laxer checks
    space_allowed_fields = ["custom_command", "config_params"]

    fields = quanting_env.model_dump()

    errors = []
    for field, value in fields.items():
        if (
            value
            and field
            not in [
                *space_allowed_fields,  # validated below
                "slurm_time",  # contains ":", validated in webapp
            ]
            and isinstance(value, str)
            and (
                errors_ := check_for_malicious_content(
                    value, allow_absolute_paths=field in absolute_path_allowed_fields
                )
            )
        ):
            errors.append(f"Validation error in '{value}': {errors_}")

    for field in space_allowed_fields:
        if fields[field]:
            errors.extend(
                check_for_malicious_content(
                    str(fields[field]),
                    allow_spaces=True,
                    allow_absolute_paths=True,
                )
            )
    if settings.config_params:
        errors.extend(
            check_for_malicious_content(
                substitute_dummy_values(settings.config_params), allow_spaces=True
            )
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


def submit_job(
    *,
    quanting_env_dict: dict,
) -> str:
    """Run a job on the cluster.

    :param quanting_env_dict: The quanting environment as a dict, as received via XCom.
    :return: The Slurm job ID as a string.
    """
    quanting_env = QuantingEnv.from_dict(quanting_env_dict)

    logging.info(f"Starting quanting with environment: {quanting_env}")

    raw_file = get_raw_file_by_id(quanting_env.raw_file_id)

    if get_instrument_settings(raw_file.instrument_id, InstrumentKeys.SKIP_QUANTING):
        logging.info(
            f"Skipping quanting for raw file {raw_file.id} because instrument settings have skip_quanting=True."
        )
        raise AirflowSkipException("Skipping quanting due to instrument settings.")

    # upfront check 2
    output_path = get_internal_output_path() / quanting_env.relative_output_path
    if output_path.exists():
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
        elif output_exists_mode == "add":
            # Normally unreachable: prepare_job already suffixed the path to a non-existent name.
            raise AirflowFailException(
                f"{msg} although Airflow variable output_exists_mode='add' should have created a unique name."
            )
        else:
            raise AirflowFailException(
                f"{msg} Remove it before restarting the quanting or set Airflow variable 'output_exists_mode' "
                f"to 'overwrite', 'associate', or 'add' (got '{output_exists_mode}')"
            )

    output_path.mkdir(parents=True, exist_ok=True)

    job_id = start_job(
        quanting_env,
        engine=quanting_env.job_engine,
    )

    # TODO: race condition here, e.g. for file in ERROR status
    update_raw_file(quanting_env.raw_file_id, new_status=RawFileStatus.QUANTING)

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


def check_job_result(*, quanting_env_dict: dict, job_id: str, ti: TaskInstance) -> dict:
    """Get info (slurm log, alphaDIA log) about a job from the cluster.

    :param quanting_env_dict: The quanting environment as a dict, as received via XCom.
    :param job_id: The Slurm job ID to check.
    :param ti: The Airflow TaskInstance, used to push error details to XCom.
    :return: Dict with ``time_elapsed`` on success.
    :raises AirflowSkipException: On known job failures (skips downstream tasks).
    :raises AirflowFailException: On unknown job failures.
    """
    quanting_env = QuantingEnv.from_dict(quanting_env_dict)

    job_status, time_elapsed = get_job_result(job_id, engine=quanting_env.job_engine)

    logging.info(f"Job {job_id} exited with status {job_status}.")

    # treat UNKNOWN (for slurm: neither scontrol nor sacct could determine the state) as a successful
    # completion: if the guess is wrong, downstream tasks fail anyway on the missing outputs
    if job_status in (JobStates.COMPLETED, JobStates.UNKNOWN):
        return {"time_elapsed": time_elapsed}

    if job_status in [JobStates.FAILED, JobStates.TIMEOUT] or job_status.startswith(
        JobStates.OUT_OF_MEMORY
    ):
        raw_file = get_raw_file_by_id(quanting_env.raw_file_id)
        output_path = get_internal_output_path() / quanting_env.relative_output_path

        if job_status == JobStates.FAILED:
            if quanting_env.software_type == SoftwareTypes.ALPHADIA:
                errors = get_business_errors(raw_file, output_path)
            else:
                errors = ["FAILED"]
        elif job_status == JobStates.TIMEOUT:
            errors = ["TIMEOUT"]
        else:
            # TODO: this seems not quite right
            errors = ["OUT_OF_MEMORY"]

        add_metrics_to_raw_file(
            raw_file.id,
            metrics={TIME_ELAPSED_METRIC: time_elapsed},
            settings_name=quanting_env.settings_name,
            settings_version=quanting_env.settings_version,
            metrics_type=quanting_env.metrics_type,
            output_path=quanting_env.output_path,
        )

        # fail the DAG without retry on new errors to make them transparent in Airflow UI
        states_to_fail_task = [
            CustomAlphaDiaStates.UNKNOWN_ERROR,
            CustomAlphaDiaStates.NO_LOG_FILE,
            CustomAlphaDiaStates.COULD_NOT_DETERMINE_ERROR,
        ]
        if any(state in errors for state in states_to_fail_task):
            put_xcom(ti, key=XComKeys.BRANCH_ERRORS, value=";".join(errors))
            raise QuantingFailedNewErrorException(f"new error: {errors=}")

        put_xcom(ti, key=XComKeys.BRANCH_ERRORS, value=";".join(errors))
        raise QuantingFailedKnownErrorException(f"known error: {errors=}")

    # unknown state: fail the DAG without retry
    put_xcom(ti, key=XComKeys.BRANCH_ERRORS, value=f"unknown_job_status: {job_status}")
    raise QuantingFailedUnknownErrorException(f"unknown_job_status: {job_status}")


def compute_metrics(
    *,
    quanting_env_dict: dict,
    time_elapsed: int | None = None,
) -> dict:
    """Compute metrics from the quanting results.

    :param quanting_env_dict: The quanting environment as a dict, as received via XCom.
    :param time_elapsed: Elapsed time from the quanting job, added to metrics if provided.
    :return: The metrics.
    """
    quanting_env = QuantingEnv.from_dict(quanting_env_dict)

    metrics_type = quanting_env.metrics_type
    output_path = get_internal_output_path() / quanting_env.relative_output_path

    metrics = calc_metrics(output_path, metrics_type=metrics_type)

    if time_elapsed is not None:  # TODO: find a better way to handle this also for msqc
        metrics[TIME_ELAPSED_METRIC] = time_elapsed

    return metrics


def store_metrics(*, quanting_env_dict: dict, metrics: dict) -> None:
    """Store metrics in the database.

    :param quanting_env_dict: The quanting environment as a dict, as received via XCom.
    :param metrics: The metrics.
    """
    quanting_env = QuantingEnv.from_dict(quanting_env_dict)

    add_metrics_to_raw_file(
        quanting_env.raw_file_id,
        metrics_type=quanting_env.metrics_type,
        metrics=metrics,
        settings_name=quanting_env.settings_name,
        settings_version=quanting_env.settings_version,
        output_path=quanting_env.output_path,
    )


MAX_STATUS_DETAILS_LENGTH = 1024

_TASK_GROUP_PREFIX = f"{TaskGroups.PROCESSING}."
_PREPARE_JOB_TASK_ID = f"{_TASK_GROUP_PREFIX}{Tasks.PREPARE_JOB}"
_CHECK_RESULT_TASK_ID = f"{_TASK_GROUP_PREFIX}{Tasks.CHECK_JOB_RESULT}"


def finalize_raw_file_status(ti: TaskInstance, raw_file_id: str) -> None:
    """Set the final status for the raw file based on all pipeline branch outcomes.

    Inspects all parallel branch outcomes and sets the final status:
    - DONE if all branches succeeded
    - QUANTING_FAILED if some branches had known business errors (no Airflow failures)
    - ERROR if any branch had an Airflow failure
    """
    dag_run = ti.get_dagrun()
    all_tis = dag_run.get_task_instances()

    # Group branch task instances by map_index (non-mapped tasks have map_index=-1)
    branch_tis_by_index: dict[int, list[TaskInstance]] = defaultdict(list)
    for ti_ in all_tis:
        if ti_.task_id.startswith(_TASK_GROUP_PREFIX) and ti_.map_index >= 0:
            branch_tis_by_index[ti_.map_index].append(ti_)

    if not branch_tis_by_index:
        raise AirflowFailException("No branch task instances found in DAG run.")

    airflow_errors, business_errors = _extract_errors(branch_tis_by_index, ti)

    if airflow_errors:
        all_errors = airflow_errors + business_errors
        details = _build_status_details(all_errors)
        logging.info(
            f"{len(airflow_errors)} branch(es) failed for {raw_file_id}: {details}"
        )
        update_raw_file(
            raw_file_id, new_status=RawFileStatus.ERROR, status_details=details
        )
        raise QuantingFailedException(details)

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
        # this is just to find such tasks in the UI more easily
        raise QuantingFailedKnownErrorException(details)

    update_raw_file(raw_file_id, new_status=RawFileStatus.DONE, status_details=None)


def _extract_errors(
    branch_tis_by_index: dict[int, list[TaskInstance]],
    ti: TaskInstance,
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Extract errors from previous tasks of all branches."""
    airflow_errors: list[tuple[str, str]] = []
    business_errors: list[tuple[str, str]] = []

    for idx in sorted(branch_tis_by_index):
        branch_tis = branch_tis_by_index[idx]
        quanting_env_dict = get_xcom(
            ti,
            key=XComKeys.RETURN_VALUE,
            task_ids=_PREPARE_JOB_TASK_ID,
            map_indexes=idx,
            default=None,
        )
        settings_name = (
            QuantingEnv.from_dict(quanting_env_dict).settings_name
            if quanting_env_dict
            else "n/a"
        )

        # these could be business or airflow errors
        check_job_result_error_details = get_xcom(
            ti,
            key=XComKeys.BRANCH_ERRORS,
            task_ids=_CHECK_RESULT_TASK_ID,
            map_indexes=idx,
            default=None,
        )

        failed_tasks_in_branch = [
            t for t in branch_tis if t.state == TaskInstanceState.FAILED
        ]

        if failed_tasks_in_branch:
            if not check_job_result_error_details:
                failed_task_names = ", ".join(
                    t.task_id.removeprefix(_TASK_GROUP_PREFIX)
                    for t in failed_tasks_in_branch
                )
                check_job_result_error_details = f"failed at {failed_task_names}"
            airflow_errors.append((settings_name, check_job_result_error_details))
        elif check_job_result_error_details:
            business_errors.append((settings_name, check_job_result_error_details))

    return airflow_errors, business_errors


def _build_status_details(errors: list[tuple[str, str]]) -> str:
    """Join per-branch error tuples into a single status_details string, truncating if needed."""
    details = "; ".join(f"[{name}] {err}" for name, err in errors)
    details = f"error while processing: {details}"
    if len(details) > MAX_STATUS_DETAILS_LENGTH:
        details = details[: MAX_STATUS_DETAILS_LENGTH - 3] + "..."
    return details
