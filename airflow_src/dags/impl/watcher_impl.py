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
from impl.handler_impl import add_raw_file_to_db
from impl.project_id_handler import get_unique_project_id

from shared.db.interface import get_all_project_ids, get_raw_file_names_from_db
from shared.db.models import RawFileStatus


def check_db(ti: TaskInstance, **kwargs) -> None:
    """Get all raw files that are not already in the database and push to XCom."""
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]
    instrument_data_path = get_internal_instrument_data_path(instrument_id)

    if not (directory_content := os.listdir(instrument_data_path)):
        raise ValueError("check_db: No raw files found in XCOM.")

    raw_file_names = [Path(directory).name for directory in directory_content]

    logging.info(
        f"Raw files to be checked against DB: {len(raw_file_names)} {raw_file_names}"
    )

    # TODO: when the kraken catches up after a stall, the acquisition_handler for a file could still be "queued"
    #  -> the file is not added to the DB yet. Subsequently, another acquisition_handler will be triggered here
    #  for the same file (which will then fail on add_to_db due to duplicate PK). Observe how often this occurs.
    for raw_file_name in get_raw_file_names_from_db(raw_file_names):
        logging.info(f"Raw file {raw_file_name} already in database.")
        raw_file_names.remove(raw_file_name)

    logging.info(
        f"Raw files to check project_id on: {len(raw_file_names)} {raw_file_names}"
    )

    put_xcom(ti, XComKeys.RAW_FILE_NAMES, raw_file_names)


def check_project_id(ti: TaskInstance, **kwargs) -> None:
    """Check if the raw files have a project id and ignore those that do not."""
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]
    raw_file_names = get_xcom(ti, XComKeys.RAW_FILE_NAMES)

    logging.info(
        f"Raw files to be checked on project id: {len(raw_file_names)} {raw_file_names}"
    )

    # remove all raw files that do not have a project but add them to the DB as 'ignored'
    all_project_ids = get_all_project_ids()
    for raw_file_name in raw_file_names.copy():
        if not get_unique_project_id(raw_file_name, all_project_ids):
            logging.warning(
                f"Raw file {raw_file_name} does not match any project of {all_project_ids}."
            )
            raw_file_names.remove(raw_file_name)

            # TODO: in terms of separation of concerns, moving this to the acquisition_handler could be preferable
            add_raw_file_to_db(
                instrument_id, raw_file_name, status=RawFileStatus.IGNORED
            )

    logging.info(
        f"Raw files to spawn acquisition_handler for: {len(raw_file_names)} {raw_file_names}"
    )

    put_xcom(ti, XComKeys.RAW_FILE_NAMES, raw_file_names)


def start_acquisition_handler(ti: TaskInstance, **kwargs) -> None:
    """Trigger a acquisition_handler DAG run for each passed raw file."""
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]
    dag_id = f"{Dags.ACQUISITON_HANDLER}.{instrument_id}"

    raw_file_names = get_xcom(ti, XComKeys.RAW_FILE_NAMES)

    for raw_file_name in raw_file_names:
        timestamp = datetime.now(tz=pytz.utc)
        run_id = DagRun.generate_run_id(DagRunType.MANUAL, timestamp)
        logging.info(
            f"Triggering DAG {dag_id} with run_id {run_id} and raw_file_name {raw_file_name}"
        )
        trigger_dag(
            dag_id=dag_id,
            run_id=run_id,
            conf={DagParams.RAW_FILE_NAME: raw_file_name},
            replace_microseconds=False,
        )
