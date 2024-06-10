"""Keys for accessing Dags, Tasks, etc.."""

from common.keys import InstrumentKeys

INSTRUMENTS = {
    # the toplevel keys determine the DAG name (e.g. 'acquisition_watcher.test6')
    "test1": {
        # raw data path relative to InternalPaths.APC_PATH_PREFIX, must be mounted in docker-compose.yml
        InstrumentKeys.RAW_DATA_PATH: "test1",
    },
    "test2": {
        InstrumentKeys.RAW_DATA_PATH: "test2",
    },
    "test3": {
        InstrumentKeys.RAW_DATA_PATH: "test3",
    },
}

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

    APC_PATH_PREFIX = "/opt/airflow/acquisition_pcs"


class Timings:
    """Timing constants."""

    FILE_CREATION_POKE_INTERVAL_S = 60

    QUANTING_MONITOR_POKE_INTERVAL_S = 60
