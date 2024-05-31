"""Business logic for the acquisition_handler."""

import logging
import sys
from time import sleep

from airflow.models import TaskInstance

# TODO: find a better way, this is required to unify module import between docker and bash
sys.path.insert(0, "/opt/airflow/")
from shared.keys import DagContext, DagParams, XComKeys
from shared.utils import get_xcom, put_xcom


def prepare_quanting(ti: TaskInstance, **kwargs) -> None:
    """TODO."""
    # example how to retrieve parameters from the context
    raw_file_name = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_NAME]
    logging.info(f"Got {raw_file_name=}")

    # IMPLEMENT:
    # create the alphadia inputfile and store it on the shared volume
    sleep(10)

    # push to XCOM
    put_xcom(ti, {XComKeys.RAW_FILE_NAME: raw_file_name})


def run_quanting(ti: TaskInstance, **kwargs) -> None:
    """TODO."""
    del kwargs
    # get from XCOM
    raw_file_name = get_xcom(ti, [XComKeys.RAW_FILE_NAME])[XComKeys.RAW_FILE_NAME]
    logging.info(f"Got {raw_file_name=}")

    # IMPLEMENT:
    # wait for the cluster to be ready (20% idling) -> dedicated (sensor) task
    # submit run script to the cluster
    sleep(10)


def monitor_quanting(ti: TaskInstance, **kwargs) -> None:
    """TODO."""
    del ti
    del kwargs

    # IMPLEMENT:
    # this should be a sensor task!
    # wait until cluster job is finished
    # task config: max runtime
    # error handling!
    sleep(10)


def compute_metrics(ti: TaskInstance, **kwargs) -> None:
    """TODO."""
    del ti
    del kwargs

    # IMPLEMENT:
    # compute metrics from the output files
    # store them locally (?)
    sleep(10)


def upload_metrics(ti: TaskInstance, **kwargs) -> None:
    """TODO."""
    del ti
    del kwargs

    # IMPLEMENT:
    # put metrics to the database

    from pymongo import MongoClient

    # Create a client
    client = MongoClient("mongodb://localhost:27017/")

    # Connect to your database
    db = client["your_database_name"]

    # Now you can use `db` to interact with your database
    logging.info(f"DB {db} connected!")

    sleep(10)
