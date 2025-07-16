"""Business logic for the instrument_watcher."""

import logging
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import pytz
from airflow.exceptions import AirflowFailException, DagNotFound
from airflow.models import TaskInstance
from common.constants import COLLISION_FLAG_SEP
from common.keys import DAG_DELIMITER, AirflowVars, DagParams, Dags, OpArgs, XComKeys
from common.utils import get_airflow_variable, get_xcom, put_xcom, trigger_dag_run
from file_handling import get_file_creation_timestamp, get_file_size
from impl.project_id_handler import get_unique_project_id
from raw_file_wrapper_factory import RawFileWrapperFactory, get_main_file_size_from_db

from shared.db.interface import (
    add_raw_file,
    delete_raw_file,
    get_all_project_ids,
    get_raw_files_by_names,
)
from shared.db.models import RawFileStatus


def _add_raw_file_to_db(
    raw_file_name: str,
    *,
    is_collision: bool,
    project_id: str,
    instrument_id: str,
    status: str = RawFileStatus.QUEUED_FOR_MONITORING,
) -> str:
    """Add the file to the database with initial status and basic information.

    :param raw_file_name: name of the raw file
    :param is_collision: wheter or not there is a collision between raw file names
    :param project_id: project id
    :param instrument_id: instrument id
    :param status: status of the file
    :return: the raw file id
    """
    raw_file_creation_timestamp = get_file_creation_timestamp(
        raw_file_name, instrument_id
    )

    return add_raw_file(
        raw_file_name,
        collision_flag=_get_collision_flag() if is_collision else None,
        project_id=project_id,
        instrument_id=instrument_id,
        status=status,
        creation_ts=raw_file_creation_timestamp,
    )


def get_unknown_raw_files(
    ti: TaskInstance, *, case_insensitive: bool = False, **kwargs
) -> None:
    """Get all raw files that should be considered for further processing and push to XCom.

    Due to potential file name collisions (i.e. a newly acquired file has the same name as one that is already processed),
    this is non-trivial, because the properties (size, hashsum) of a file in the DB could still change if the file
    is still being acquired.
    For each file on the instrument, we check if it is already in the database.

    If the case_insensitive flag is set, the comparison is done by case-insensitive original file name,
    i.e. the files TEST.raw and test.raw are considered the same.
    This is important, in case the backup file system is case-insensitive, e.g. on Windows.

    If not, it is kept in the list of files to be processed that is eventually pushed to XCom.
    If yes, we first check all file sizes in the DB:
        If at least one is not set, we assume the acquisition is not done, we don't process the file in this DAG run.
        It will be revisited in the next DAG run.

        If the file in the DB is fixed, we compare the current file size with the one in the DB.
            If they match, we consider the two files to be the same, and we don't process the file further.
            Eventually, it will be moved from the instrument by another component.

            If they don't match, we have found a collision, i.e. the file should be processed but also needs to be distinguished
            from the file that is already in the DB. To achieve this, later a "collision_flag" is added to the file, which serves to
            tell the file apart from the original file (and also from other collisions).

    Note there's a potential race condition with the "add to db" operation in the subsequent
    start_acquisition_handler(), task, However, as we allow only one of these DAGs to run at a time, this should not be an issue.
    """

    def _cond_lower(raw_file_name: str) -> str:
        """Return the lower case version of the file name if case-insensitive."""
        return raw_file_name.lower() if case_insensitive else raw_file_name

    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]

    raw_file_names_on_instrument = sorted(
        RawFileWrapperFactory.create_monitor_wrapper(
            instrument_id=instrument_id
        ).get_raw_files_on_instrument()
    )

    logging.info(
        f"{len(raw_file_names_on_instrument)} raw files to be checked against DB: {raw_file_names_on_instrument}"
    )

    raw_files_names_lower_to_sizes_from_db: dict[str, list[int]] = defaultdict(list)
    for raw_file in get_raw_files_by_names(list(raw_file_names_on_instrument)):
        # due to collisions, there could be more than one raw file with the same original name
        raw_file_size = get_main_file_size_from_db(raw_file)
        raw_files_names_lower_to_sizes_from_db[
            _cond_lower(raw_file.original_name)
        ].append(raw_file_size)
    logging.info(f"got {raw_files_names_lower_to_sizes_from_db=}")

    raw_file_names_to_process: dict[str, bool] = {}
    for raw_file_name_on_instrument in raw_file_names_on_instrument:
        is_collision = False

        if (
            _cond_lower(raw_file_name_on_instrument)
            in raw_files_names_lower_to_sizes_from_db
        ):
            logging.info(
                f"File in DB: {raw_file_name_on_instrument}, checking for potential collision.."
            )

            main_file_path = RawFileWrapperFactory.create_monitor_wrapper(
                instrument_id=instrument_id,
                raw_file_original_name=raw_file_name_on_instrument,
            ).main_file_path()

            is_collision = _is_collision(
                main_file_path,
                raw_files_names_lower_to_sizes_from_db[
                    _cond_lower(raw_file_name_on_instrument)
                ],
            )
            if not is_collision:
                logging.info("Not considering file for further processing in this run.")
                continue

        raw_file_names_to_process[raw_file_name_on_instrument] = is_collision

    logging.info(
        f"{len(raw_file_names_to_process)} raw files left after DB check: {raw_file_names_to_process}"
    )

    raw_file_names_sorted = _sort_by_creation_date(
        list(raw_file_names_to_process.keys()), instrument_id
    )
    raw_files_to_process_sorted = {
        r: raw_file_names_to_process[r] for r in raw_file_names_sorted
    }

    put_xcom(ti, XComKeys.RAW_FILE_NAMES_TO_PROCESS, raw_files_to_process_sorted)


def _is_collision(main_file_path: Path, sizes: list[int | None]) -> bool:
    """Detect a collision between a raw file and a file in the database.

    Returns False if there's no collision or decision not possible, otherwise True.
    """
    if any(s is None for s in sizes):
        logging.info(
            f"At least one file in DB is not fixed yet: {sizes=}, cannot decide on collision."
        )
        return False

    logging.info(f"All files in DB are fixed, checking size against {sizes=}")

    if not main_file_path.exists():
        logging.info("Main file does not exist yet.")
        return False

    current_size = get_file_size(main_file_path)
    if any(current_size == size for size in sizes):
        logging.info(
            f"At least one file size match: {current_size=} {sizes=}. Assuming it's the same file, no collision."
        )
        return False

    logging.info(f"File size mismatch {current_size=} {sizes=}. Assuming collision.")

    return True


def _sort_by_creation_date(raw_file_names: list[str], instrument_id: str) -> list[str]:
    """Sort raw files by creation timestamp (youngest first) to have them processed first."""
    file_creation_timestamps = []
    for raw_file_name in raw_file_names:
        file_creation_ts = get_file_creation_timestamp(raw_file_name, instrument_id)
        file_creation_timestamps.append(file_creation_ts)
    return [r for _, r in sorted(zip(file_creation_timestamps, raw_file_names))][::-1]


def decide_raw_file_handling(ti: TaskInstance, **kwargs) -> None:
    """Decide for each raw file whether an acquisition handler should be triggered or not."""
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]
    raw_file_names_to_process: dict[str, bool] = get_xcom(
        ti, XComKeys.RAW_FILE_NAMES_TO_PROCESS
    )

    logging.info(
        f"{len(raw_file_names_to_process)} raw files to be checked on project id: {raw_file_names_to_process}"
    )

    all_project_ids = get_all_project_ids()

    raw_file_names_with_decisions: dict[str, tuple[str, bool, bool]] = {}
    for raw_file_name, is_collision in raw_file_names_to_process.items():
        project_id = get_unique_project_id(raw_file_name, all_project_ids)

        if project_id is None:
            logging.info(
                f"Raw file {raw_file_name} does not match exactly one project of {all_project_ids}."
            )

        file_needs_handling = True
        file_needs_handling &= _file_meets_age_criterion(
            raw_file_name,
            instrument_id,
        )

        raw_file_names_with_decisions[raw_file_name] = (
            project_id,
            file_needs_handling,
            is_collision,
        )

        # here we could add more logic to decide whether to handle the file or not, e.g. a global blacklist

    logging.info(f"Got {len(raw_file_names_with_decisions)} raw files to handle.")

    put_xcom(ti, XComKeys.RAW_FILE_NAMES_WITH_DECISIONS, raw_file_names_with_decisions)


def _file_meets_age_criterion(
    raw_file_name: str,
    instrument_id: str,
) -> bool:
    """Check if the file meets the age criterion defined by the corresponding Airflow variable.

    :param raw_file_name: name of raw file
    :param instrument_id: instrument id
    :return: True if the file is younger than the given max. file age or if no max. file age defined, False otherwise
    """
    try:
        if (
            max_file_age_in_hours := float(
                get_airflow_variable(
                    AirflowVars.DEBUG_MAX_FILE_AGE_IN_HOURS,
                    default="-1",
                )
            )
        ) == -1:
            return True
    except ValueError as e:
        raise ValueError("Could not convert max_file_age_in_hours to float.") from e

    file_creation_ts = get_file_creation_timestamp(raw_file_name, instrument_id)
    raw_file_creation_time = datetime.fromtimestamp(file_creation_ts, tz=pytz.utc)

    if (now := datetime.now(tz=pytz.utc)) - raw_file_creation_time > timedelta(
        hours=max_file_age_in_hours
    ):  # TODO: check time zone on acquisition PCS
        logging.info(
            f"File {raw_file_name} is too old: {now=} {raw_file_creation_time=}"
        )
        return False

    return True


def _get_collision_flag() -> str:
    """Get a collision flag to resolve file name collisions."""
    # the collision flag needs to be "unique enough", is used only to tell different collisions apart
    timestamp = datetime.now(tz=pytz.utc).strftime("%Y%m%d-%H%M%S-%f")
    collision_flag = f"{timestamp}{COLLISION_FLAG_SEP}"
    logging.info(f"Adding {collision_flag=}")
    return collision_flag


def start_acquisition_handler(ti: TaskInstance, **kwargs) -> None:
    """Trigger an acquisition_handler DAG run for specific raw files.

    Each raw file is added to the database first.
    Then, for each raw file, the project id is determined.
    Only for raw files that carry a project id, the acquisition_handler DAG is triggered.
    """
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]
    raw_file_names_with_decisions = get_xcom(ti, XComKeys.RAW_FILE_NAMES_WITH_DECISIONS)
    logging.info(f"Got {len(raw_file_names_with_decisions)} raw files to handle.")

    dag_id_to_trigger = f"{Dags.ACQUISITION_HANDLER}{DAG_DELIMITER}{instrument_id}"

    for raw_file_name, (
        project_id,
        file_needs_handling,
        is_collision,
    ) in raw_file_names_with_decisions.items():
        status = (
            RawFileStatus.QUEUED_FOR_MONITORING
            if file_needs_handling
            else RawFileStatus.IGNORED
        )

        # Here mongoengine.errors.NotUniqueError is raised when the file is already in the DB.
        # It is deliberately not caught: on this error, the task will fail, but in the next DAG run, the
        # file that caused the problem is filtered out in get_unknown_raw_files().
        # Beware: if `is_collision` is `True`, and this task is re-run, it will be successfully saved and processed
        # as a collision. So avoid manual restarts of this task.
        # To prevent automatic task restarting, retries need to be set to 0.
        raw_file_id = _add_raw_file_to_db(
            raw_file_name,
            is_collision=is_collision,
            project_id=project_id,
            instrument_id=instrument_id,
            status=status,
        )

        if not file_needs_handling:
            logging.info(
                f"Not triggering DAG {dag_id_to_trigger} for {raw_file_name=}."
            )
            continue

        # Adding the files to the DB and triggering the acquisition_handler DAG must be an atomic transaction.
        # To ensure atomicity of DB entry and DAG triggering, all operations on a single file need to be successful or none of them.
        # In case of an error, the file is deleted from the DB again (=rollback).
        try:
            trigger_dag_run(
                dag_id_to_trigger,
                {DagParams.RAW_FILE_ID: raw_file_id},
            )
        except DagNotFound as e:
            # this happens very rarely, but if not handled here, the file would need to be removed from the DB manually
            logging.exception(
                f"DAG {dag_id_to_trigger} not found. Removing file from DB again."
            )
            delete_raw_file(raw_file_id)

            # raising here to make this error transparent
            raise AirflowFailException(
                f"DAG {dag_id_to_trigger} not found. File {raw_file_id} will be picked up again in next DAG run."
            ) from e
