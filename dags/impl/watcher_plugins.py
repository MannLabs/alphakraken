"""Business logic for the acquisition_watcher."""

import logging
import sys
from time import sleep

from airflow.models import TaskInstance

# TODO: find a better way, this is required to unify module import between docker and bash
sys.path.insert(0, "/opt/airflow/")
from shared.keys import OpArgs


def wait_for_finished_acquisition(ti: TaskInstance, **kwargs) -> None:
    """Wait for the acquisition to finish."""
    del ti
    # example how to retrieve parameters
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]

    logging.info(f"Got {instrument_id=}")

    # some logic ..
    sleep(10)
