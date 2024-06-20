"""Business logic for the acquisition_watcher."""

import logging
import os
from datetime import datetime
from pathlib import Path

import pytz
from airflow.api.common.trigger_dag import trigger_dag
from airflow.models import DagRun, TaskInstance
from airflow.utils.types import DagRunType
from common.keys import DagParams, Dags, OpArgs, XComKeys
from common.settings import get_internal_instrument_data_path
from common.utils import get_xcom, put_xcom
from impl.project_id_handler import get_unique_project_id

from shared.db.interface import (
    add_new_raw_file_to_db,
    get_all_project_ids,
    get_raw_file_names_from_db,
)
from shared.db.models import RawFileStatus


def _add_raw_file_to_db(
    instrument_id: str, raw_file_name: str, status: str = RawFileStatus.NEW
) -> None:
    """Add the file to the database with initial status and basic information.

    :param instrument_id: instrument id
    :param raw_file_name: raw file name
    :param status: status of the file
    :return:
    """
    # calculate the file properties already here to have them available as early as possible
    raw_file_path = get_internal_instrument_data_path(instrument_id) / raw_file_name
    raw_file_size = raw_file_path.stat().st_size
    raw_file_creation_time = raw_file_path.stat().st_ctime
    logging.info(f"Got {raw_file_size / 1024 ** 3} GB {raw_file_creation_time}")

    add_new_raw_file_to_db(
        raw_file_name,
        status=status,
        instrument_id=instrument_id,
        size=raw_file_size,
        creation_ts=raw_file_creation_time,
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

    # Note there's a potential race condition with the "add to db" operation in start_acquisition_handler(),
    # however, as we allow only one of these DAGs to run at a time, this should not be an issue.
    for raw_file_name in get_raw_file_names_from_db(raw_file_names):
        logging.info(f"Raw file {raw_file_name} already in database.")
        raw_file_names.remove(raw_file_name)

    logging.info(
        f"Raw files left after DB check: {len(raw_file_names)} {raw_file_names}"
    )

    put_xcom(ti, XComKeys.RAW_FILE_NAMES, raw_file_names)


def decide_raw_file_handling(ti: TaskInstance, **kwargs) -> None:
    """Decide for each raw file wheter a acquisition handler should be triggered or not."""
    del kwargs  # unused
    raw_file_names = get_xcom(ti, XComKeys.RAW_FILE_NAMES)

    logging.info(
        f"Raw files to be checked on project id: {len(raw_file_names)} {raw_file_names}"
    )

    all_project_ids = get_all_project_ids()

    raw_file_handling_decisions: dict[str, bool] = {}
    for raw_file_name in raw_file_names:
        project_id = get_unique_project_id(raw_file_name, all_project_ids)

        if project_id is None:
            logging.warning(
                f"Raw file {raw_file_name} does not match exactly one project of {all_project_ids}."
            )

        raw_file_handling_decisions[raw_file_name] = True

        # here we could add more logic to decide whether to handle the file or not, e.g. a global blacklist

    put_xcom(ti, XComKeys.RAW_FILE_HANDLING_DECISIONS, raw_file_handling_decisions)


def start_acquisition_handler(ti: TaskInstance, **kwargs) -> None:
    """Trigger a acquisition_handler DAG run for specific raw files.

    Each raw file is added to the database first.
    Then, for each raw file, the project id is determined.
    Only for raw files that carry a project id, the acquisition_handler DAG is triggered.
    """
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]
    raw_file_handling_decisions = get_xcom(ti, XComKeys.RAW_FILE_HANDLING_DECISIONS)

    dag_id_to_trigger = f"{Dags.ACQUISITON_HANDLER}.{instrument_id}"

    # adding the files to the DB and triggering the acquisition_handler DAG should be atomic
    for raw_file_name, is_to_be_handled in raw_file_handling_decisions.items():
        status = RawFileStatus.NEW if is_to_be_handled else RawFileStatus.IGNORED

        _add_raw_file_to_db(instrument_id, raw_file_name, status=status)

        if is_to_be_handled:
            run_id = DagRun.generate_run_id(
                DagRunType.MANUAL, execution_date=datetime.now(tz=pytz.utc)
            )
            logging.info(
                f"Triggering DAG {dag_id_to_trigger} with run_id {run_id} for raw_file_name {raw_file_name}."
            )
            trigger_dag(
                dag_id=dag_id_to_trigger,
                run_id=run_id,
                conf={DagParams.RAW_FILE_NAME: raw_file_name},
                replace_microseconds=False,
            )
