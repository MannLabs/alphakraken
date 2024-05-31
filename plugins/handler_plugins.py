"""Business logic for the acquisition_handler."""

import logging
from time import sleep

from airflow.models import TaskInstance

from shared.keys import DagContext, DagParams, XComKeys
from shared.utils import get_xcom, put_xcom


def prepare_quanting(ti: TaskInstance, **kwargs) -> None:
    """TODO."""
    # example how to retrieve parameters from the context
    raw_file_name = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_NAME]
    logging.info(f"Got {raw_file_name=}")

    # some logic ..
    sleep(10)

    # push to XCOM
    put_xcom(ti, {XComKeys.RAW_FILE_NAME: raw_file_name})


def run_quanting(ti: TaskInstance, **kwargs) -> None:
    """TODO."""
    del kwargs
    # get from XCOM
    raw_file_name = get_xcom(ti, [XComKeys.RAW_FILE_NAME])[XComKeys.RAW_FILE_NAME]
    logging.info(f"Got {raw_file_name=}")

    # some logic ..
    sleep(10)


def monitor_quanting(ti: TaskInstance, **kwargs) -> None:
    """TODO."""
    del ti
    del kwargs
    # some logic ..
    sleep(10)


def compute_metrics(ti: TaskInstance, **kwargs) -> None:
    """TODO."""
    del ti
    del kwargs
    # some logic ..
    sleep(10)


def upload_metrics(ti: TaskInstance, **kwargs) -> None:
    """TODO."""
    del ti
    del kwargs
    # some logic ..
    sleep(10)
