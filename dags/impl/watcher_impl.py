"""Business logic for the acquisition_watcher."""
# ruff: noqa: E402  # Module level import not at top of file

import logging
import sys
from pathlib import Path
from time import sleep

from airflow.models import TaskInstance

# TODO: find a better way to unify import of modules 'dags', 'shared', ... between docker and standalone
root_path = str(Path(__file__).parent / Path(".."))
if root_path not in sys.path:
    sys.path.insert(0, root_path)
from shared.keys import OpArgs


def wait_for_finished_acquisition(ti: TaskInstance, **kwargs) -> None:
    """Wait for the acquisition to finish."""
    del ti
    # example how to retrieve parameters
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]

    logging.info(f"Got {instrument_id=}")

    # some logic ..
    sleep(10)
