"""Business logic for the acquisition_handler."""

import logging
import sys

from airflow.models import TaskInstance

# TODO: find a better way, this is required to unify module import between docker and bash
sys.path.insert(0, "/opt/airflow/")
from shared.db.client import MongoDBClient
from shared.db.models import RawFile
from shared.keys import DagContext, DagParams, XComKeys
from shared.settings import RawFileStatus
from shared.utils import get_xcom, put_xcom


def add_to_db(ti: TaskInstance, **kwargs) -> None:
    """TODO."""
    # example how to retrieve parameters from the context
    raw_file_name = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_NAME]
    logging.info(f"Got {raw_file_name=}")

    client = MongoDBClient()

    raw_file = RawFile(raw_file_name, RawFileStatus.NEW)

    # example: insert data to DB
    x = client.insert(raw_file)
    logging.info(f"Inserted item {x}")

    # example: get data from DB
    raw_file_to_query = RawFile(raw_file_name)
    y = client.count(raw_file_to_query)
    logging.info(f"count in DB: {y}")

    z = client.find(raw_file_to_query)
    logging.info(f"got from DB: {z}")

    # IMPLEMENT:
    # create the alphadia inputfile and store it on the shared volume

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
    del kwargs

    # example: update raw file status
    raw_file_name = get_xcom(ti, XComKeys.RAW_FILE_NAME)
    raw_file = RawFile(raw_file_name, RawFileStatus.PROCESSING)
    MongoDBClient().update(raw_file)

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

    # example: update raw file status
    raw_file_name = get_xcom(ti, XComKeys.RAW_FILE_NAME)
    raw_file = RawFile(raw_file_name, RawFileStatus.PROCESSED)
    MongoDBClient().update(raw_file)

    # sanity check:
    z = MongoDBClient().find(raw_file)
    logging.info(f"got from DB: {z}")

    # IMPLEMENT:
    # put metrics to the database
