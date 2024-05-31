"""Business logic for the acquisition_watcher."""

import logging
from time import sleep

from airflow.models import TaskInstance

from shared.keys import OpArgs


def wait_for_finished_acquisition(ti: TaskInstance, **kwargs) -> None:
    """Wait for the acquisition to finish."""
    del ti
    # example how to retrieve parameters
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]

    logging.info(f"Got {instrument_id=}")

    # some logic ..
    sleep(10)
