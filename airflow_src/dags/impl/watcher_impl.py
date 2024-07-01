"""Business logic for the acquisition_watcher."""

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

import pytz
from airflow.models import TaskInstance
from common.keys import AirflowVars, DagParams, Dags, OpArgs, XComKeys
from common.settings import get_internal_instrument_data_path
from common.utils import get_airflow_variable, get_xcom, put_xcom, trigger_dag_run
from impl.project_id_handler import get_unique_project_id

from shared.db.interface import (
    add_new_raw_file_to_db,
    get_all_project_ids,
    get_raw_file_names_from_db,
)
from shared.db.models import RawFileStatus


def _add_raw_file_to_db(
    raw_file_name: str,
    *,
    project_id: str,
    instrument_id: str,
    status: str = RawFileStatus.NEW,
) -> None:
    """Add the file to the database with initial status and basic information.

    :param raw_file_name: raw file name
    :param project_id: project id
    :param instrument_id: instrument id
    :param status: status of the file
    :return:
    """
    raw_file_creation_timestamp = _get_file_creation_timestamp(
        raw_file_name, instrument_id
    )
    logging.info(f"Got  {raw_file_creation_timestamp}")

    add_new_raw_file_to_db(
        raw_file_name,
        project_id=project_id,
        instrument_id=instrument_id,
        status=status,
        creation_ts=raw_file_creation_timestamp,
    )


def get_unknown_raw_files(ti: TaskInstance, **kwargs) -> None:
    """Get all raw files that are not already in the database and push to XCom."""
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]
    instrument_data_path = get_internal_instrument_data_path(instrument_id)

    if not (directory_content := os.listdir(instrument_data_path)):
        raise ValueError("get_unknown_raw_files: No raw files found in XCOM.")

    raw_file_names = [Path(directory).name for directory in directory_content]

    logging.info(
        f"Raw files to be checked against DB: {len(raw_file_names)} {raw_file_names}"
    )

    # Note there's a potential race condition with the "add to db" operation in start_file_handler(),
    # however, as we allow only one of these DAGs to run at a time, this should not be an issue.
    for raw_file_name in get_raw_file_names_from_db(raw_file_names):
        logging.info(f"Raw file {raw_file_name} already in database.")
        raw_file_names.remove(raw_file_name)

    logging.info(
        f"Raw files left after DB check: {len(raw_file_names)} {raw_file_names}"
    )

    raw_file_names_sorted = _sort_by_creation_date(raw_file_names, instrument_id)

    put_xcom(ti, XComKeys.RAW_FILE_NAMES, raw_file_names_sorted)


def _sort_by_creation_date(raw_file_names: list[str], instrument_id: str) -> list[str]:
    """Sort raw files by creation timestamp (youngest first) to have them processed first."""
    file_creation_timestamps = []
    for raw_file_name in raw_file_names:
        file_creation_ts = _get_file_creation_timestamp(raw_file_name, instrument_id)
        file_creation_timestamps.append(file_creation_ts)
    return [r for _, r in sorted(zip(file_creation_timestamps, raw_file_names))][::-1]


def decide_raw_file_handling(ti: TaskInstance, **kwargs) -> None:
    """Decide for each raw file wheter a acquisition handler should be triggered or not."""
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]
    raw_file_names = get_xcom(ti, XComKeys.RAW_FILE_NAMES)

    logging.info(
        f"Raw files to be checked on project id: {len(raw_file_names)} {raw_file_names}"
    )

    all_project_ids = get_all_project_ids()

    raw_file_project_ids: dict[str, tuple[str, bool]] = {}
    for raw_file_name in raw_file_names:
        project_id = get_unique_project_id(raw_file_name, all_project_ids)

        if project_id is None:
            logging.warning(
                f"Raw file {raw_file_name} does not match exactly one project of {all_project_ids}."
            )

        file_needs_handling = True
        file_needs_handling &= _file_meets_age_criterion(
            raw_file_name,
            instrument_id,
        )

        raw_file_project_ids[raw_file_name] = (project_id, file_needs_handling)

        # here we could add more logic to decide whether to handle the file or not, e.g. a global blacklist

    logging.info(f"Got {len(raw_file_project_ids)} raw files to handle.")

    put_xcom(ti, XComKeys.RAW_FILE_PROJECT_IDS, raw_file_project_ids)


def _file_meets_age_criterion(
    raw_file_name: str,
    instrument_id: str,
) -> bool:
    """Check if the file meets the age criterion defined by the corresponding Airflow variable.

    :param raw_file_name: name of raw file
    :param instrument_id: instrument id
    :return: True if the file is younger than the given max. file age or if no max. file age defined, False otherwise
    """
    max_file_age_in_hours_not_active = "-1"
    max_file_age_in_hours: str = get_airflow_variable(
        AirflowVars.MAX_FILE_AGE_IN_HOURS, max_file_age_in_hours_not_active
    )

    try:
        max_file_age_in_hours_float = float(max_file_age_in_hours)
    except ValueError as e:
        logging.exception(
            f"Could not convert max_file_age_in_hours to float: {max_file_age_in_hours}"
        )
        raise ValueError from e

    if max_file_age_in_hours != max_file_age_in_hours_not_active:
        file_creation_ts = _get_file_creation_timestamp(raw_file_name, instrument_id)
        raw_file_creation_time = datetime.fromtimestamp(file_creation_ts, tz=pytz.utc)

        now = datetime.now(tz=pytz.utc)  # TODO: check time zone on acquisition PCS
        time_delta = timedelta(hours=max_file_age_in_hours_float)
        logging.info(f"{now=} {raw_file_creation_time=} {time_delta=}")
        if now - raw_file_creation_time > time_delta:
            logging.info(f"File {raw_file_name} is too old.")
            return False

    return True


def _get_file_size(raw_file_name: str, instrument_id: str) -> float:
    """Get the size (in bytes) of a raw file."""
    raw_file_path = get_internal_instrument_data_path(instrument_id) / raw_file_name
    file_size_bytes = raw_file_path.stat().st_size
    logging.info(f"File {raw_file_name} has {file_size_bytes=}")
    return file_size_bytes


def _get_file_creation_timestamp(raw_file_name: str, instrument_id: str) -> float:
    """Get the creation timestamp (unix epoch) of a raw file."""
    raw_file_path = get_internal_instrument_data_path(instrument_id) / raw_file_name
    file_creation_ts = raw_file_path.stat().st_ctime
    logging.info(f"File {raw_file_name} has {file_creation_ts=}")
    return file_creation_ts


def start_file_handler(ti: TaskInstance, **kwargs) -> None:
    """Trigger a file_handler DAG run for specific raw files.

    Each raw file is added to the database first.
    Then, for each raw file, the project id is determined.
    Only for raw files that carry a project id, the file_handler DAG is triggered.
    """
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]
    raw_file_project_ids = get_xcom(ti, XComKeys.RAW_FILE_PROJECT_IDS)
    logging.info(f"Got {len(raw_file_project_ids)} raw files to handle.")

    dag_id_to_trigger = f"{Dags.FILE_HANDLER}.{instrument_id}"

    # adding the files to the DB and triggering the file_handler DAG should be atomic
    for raw_file_name, (
        project_id,
        file_needs_handling,
    ) in raw_file_project_ids.items():
        status = (RawFileStatus.NEW) if file_needs_handling else RawFileStatus.IGNORED

        # putting the file name to xcom to be able to access it in callback for error reporting
        put_xcom(ti, XComKeys.RAW_FILE_NAME, raw_file_name)

        # TODO: fix: if this task is restarted, this could give a `mongoengine.errors.NotUniqueError`
        _add_raw_file_to_db(
            raw_file_name,
            project_id=project_id,
            instrument_id=instrument_id,
            status=status,
        )

        if file_needs_handling:
            trigger_dag_run(
                dag_id_to_trigger,
                {
                    DagParams.RAW_FILE_NAME: raw_file_name,
                },
            )
        else:
            logging.info(
                f"Not triggering DAG {dag_id_to_trigger} for {raw_file_name=}."
            )
