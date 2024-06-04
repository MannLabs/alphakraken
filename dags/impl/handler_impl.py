"""Business logic for the acquisition_handler."""
# ruff: noqa: E402  # Module level import not at top of file

import logging
import sys
from pathlib import Path

from airflow.models import TaskInstance

# TODO: find a better way to unify import of modules 'dags', 'shared', ... between docker and standalone
root_path = str(Path(__file__).parent / Path(".."))
if root_path not in sys.path:
    sys.path.insert(0, root_path)
from shared.db.engine import RawFile, connect_db
from shared.keys import DagContext, DagParams, XComKeys
from shared.settings import RawFileStatus
from shared.utils import get_xcom, put_xcom


def add_to_db(ti: TaskInstance, **kwargs) -> None:
    """TODO."""
    # example how to retrieve parameters from the context
    raw_file_name = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_NAME]
    logging.info(f"Got {raw_file_name=}")

    connect_db()
    raw_file = RawFile(name=raw_file_name, status=RawFileStatus.NEW)

    # example: insert data to DB
    raw_file.save(force_insert=True)

    # example: get data from DB

    for raw_file in RawFile.objects:
        logging.info(raw_file.name)

    for raw_file in RawFile.objects(name=raw_file_name):
        logging.info(raw_file.name)

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
