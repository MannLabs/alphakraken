"""Keys for accessing Dags, Tasks, etc.."""

from shared.keys import InstrumentKeys

INSTRUMENTS = {
    # the toplevel keys determine the DAG name (e.g. 'acquisition_watcher.test6')
    "test6": {
        # raw data path relative to InternalPaths.APC_PATH_PREFIX, must be mounted in docker-compose.yml
        InstrumentKeys.RAW_DATA_PATH: "apc_tims_1",
    },
    "test7": {
        InstrumentKeys.RAW_DATA_PATH: "apc_tims_2",
    },
}


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
