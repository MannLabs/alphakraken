"""Business logic for the acquisition_handler."""
# ruff: noqa: E402  # Module level import not at top of file

import logging

from airflow.models import TaskInstance
from common.keys import DagContext, DagParams, OpArgs, XComKeys
from common.settings import RawFileStatus
from common.utils import get_instrument_data_path, get_xcom, put_xcom

from shared.db.engine import RawFile, add_new_raw_file_to_db, connect_db


def add_to_db(ti: TaskInstance, **kwargs) -> None:
    """Add the file to the database with initial status and basic information."""
    # example how to retrieve parameters from the context
    raw_file_name = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_NAME]
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]

    logging.info(f"Got {raw_file_name=} on {instrument_id=}")

    raw_file_path = get_instrument_data_path(instrument_id) / raw_file_name
    raw_file_size = raw_file_path.stat().st_size / 1024**3
    logging.info(f"Got {raw_file_size=} GB")

    add_new_raw_file_to_db(
        raw_file_name, instrument_id=instrument_id, raw_file_size=raw_file_size
    )

    # push to XCOM
    put_xcom(ti, XComKeys.RAW_FILE_NAME, raw_file_name)


def prepare_quanting(ti: TaskInstance, **kwargs) -> None:
    """TODO."""
    del ti
    del kwargs
    # IMPLEMENT:
    # create the alphadia inputfile and store it on the shared volume


def run_quanting(ti: TaskInstance, **kwargs) -> None:
    """TODO."""
    del ti
    del kwargs

    # IMPLEMENT:
    # wait for the cluster to be ready (20% idling) -> dedicated (sensor) task
    # submit run script to the cluster


def monitor_quanting(ti: TaskInstance, **kwargs) -> None:
    """TODO."""
    del ti
    del kwargs

    # IMPLEMENT:
    # this should be a sensor task!
    # wait until cluster job is finished
    # task config: max runtime
    # error handling!


def compute_metrics(ti: TaskInstance, **kwargs) -> None:
    """TODO."""
    del ti
    del kwargs

    # IMPLEMENT:
    # compute metrics from the output files
    # store them locally (?)


def upload_metrics(ti: TaskInstance, **kwargs) -> None:
    """TODO."""
    del kwargs

    raw_file_name = get_xcom(ti, XComKeys.RAW_FILE_NAME)
    connect_db()

    raw_file = RawFile.objects.with_id(raw_file_name)
    # example: update raw file status

    logging.info(f"got {raw_file=}")
    raw_file.update(status=RawFileStatus.PROCESSED)

    # sanity check:
    for raw_file in RawFile.objects(name=raw_file_name):
        logging.info(f"{raw_file.name} {raw_file.status}")

    # IMPLEMENT:
    # put metrics to the database
