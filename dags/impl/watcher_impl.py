"""Business logic for the acquisition_watcher."""
# ruff: noqa: E402  # Module level import not at top of file

import logging
import os
import sys
from datetime import datetime
from pathlib import Path

import pytz
from airflow.models import DagRun, TaskInstance
from airflow.utils.types import DagRunType

from plugins.common.keys import DagParams, Dags, OpArgs, XComKeys
from plugins.common.utils import get_instrument_data_path, get_xcom, put_xcom

# TODO: find a better way to unify import of modules 'dags', 'common', ... between docker and standalone
root_path = str(Path(__file__).parent / Path("../.."))
if root_path not in sys.path:
    sys.path.insert(0, root_path)
from shared.db.engine import get_raw_file_names_from_db


def get_raw_files(ti: TaskInstance, **kwargs) -> None:
    """Get all raw files that are not already in the database and push to XCom."""
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]
    instrument_data_path = get_instrument_data_path(instrument_id)

    if not (directory_content := os.listdir(instrument_data_path)):
        raise ValueError("get_raw_files: No raw files found in XCOM.")

    raw_file_names = [Path(directory).name for directory in directory_content]

    logging.info(f"Raw files to be checked: {len(raw_file_names)} {raw_file_names}")

    for raw_file_name in get_raw_file_names_from_db(raw_file_names):
        logging.info(f"Raw file {raw_file_name} already in database.")
        raw_file_names.remove(raw_file_name)

    logging.info(f"Raw files to be processed: {len(raw_file_names)} {raw_file_names}")

    put_xcom(ti, XComKeys.RAW_FILE_NAMES, raw_file_names)


from airflow.api.common.trigger_dag import trigger_dag


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
