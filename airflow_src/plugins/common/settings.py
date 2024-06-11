"""Keys for accessing Dags, Tasks, etc.."""

from pathlib import Path

from common.keys import InstrumentKeys
from common.utils import get_environment_variable

INSTRUMENTS = {
    # the toplevel keys determine the DAG name (e.g. 'acquisition_watcher.test6')
    "test1": {
        # raw data path relative to InternalPaths.MOUNTS_PATH which must be mounted in docker-compose.yml
        # TODO: fix: we need to provide a default here to make test_dags.py happy. Should not be an issue in production.
        InstrumentKeys.RAW_DATA_PATH: get_environment_variable(
            "INSTRUMENT_PATH_TEST1", "n_a"
        ),
    },
    "test2": {
        InstrumentKeys.RAW_DATA_PATH: get_environment_variable(
            "INSTRUMENT_PATH_ASTRAL1", "n_a"
        ),
    },
    "test3": {
        InstrumentKeys.RAW_DATA_PATH: get_environment_variable(
            "INSTRUMENT_PATH_ASTRAL2", "n_a"
        ),
    },
}


def get_instrument_data_path(instrument_id: str) -> Path:
    """Get internal path for the given instrument."""
    return (
        Path(InternalPaths.MOUNTS_PATH)
        / INSTRUMENTS[instrument_id][InstrumentKeys.RAW_DATA_PATH]
    )


# prefix for the queues the DAGs are assigned to (cf. docker-compose.yml)
AIRFLOW_QUEUE_PREFIX = "kraken_queue_"


class RawFileStatus:
    """Status of raw file."""

    NEW = "new"
    # have a distinction between processing and copying as network drives caused issues in the past.
    COPYING = "copying"
    PROCESSING = "processing"
    PROCESSED = "processed"
    FAILED = "failed"


class InternalPaths:
    """Paths to directories."""

    MOUNTS_PATH = "/opt/airflow/mounts/"


class Timings:
    """Timing constants."""

    FILE_CREATION_POKE_INTERVAL_S = 60

    QUANTING_MONITOR_POKE_INTERVAL_S = 60
