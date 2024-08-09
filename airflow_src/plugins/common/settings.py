"""Keys for accessing Dags, Tasks, etc.."""

from pathlib import Path

from common.keys import InstrumentKeys, InstrumentTypes

INSTRUMENTS = {
    # the toplevel keys determine the DAG name (e.g. 'instrument_watcher.test1')
    "test1": {
        InstrumentKeys.TYPE: InstrumentTypes.THERMO,
    },
    "test2": {
        InstrumentKeys.TYPE: InstrumentTypes.THERMO,
    },
}

# prefix for the queues the DAGs are assigned to (cf. docker-compose.yaml)
AIRFLOW_QUEUE_PREFIX = "kraken_queue_"

OUTPUT_FOLDER_PREFIX = "out_"


CLUSTER_BASE_DIR = "~/slurm"
CLUSTER_JOB_SCRIPT_PATH = f"{CLUSTER_BASE_DIR}/submit_job.sh"
CLUSTER_WORKING_DIR = f"{CLUSTER_BASE_DIR}/jobs"

FALLBACK_PROJECT_ID = "_FALLBACK"
FALLBACK_PROJECT_ID_BRUKER = "_FALLBACK_BRUKER"

COLLISION_FLAG_SEP = "---"


class InternalPaths:
    """Paths to directories within the Docker containers."""

    MOUNTS_PATH = "/opt/airflow/mounts/"
    INSTRUMENTS = "instruments"
    BACKUP = "backup"
    OUTPUT = "output"


class Timings:
    """Timing constants."""

    # if you update this, you might also want to update the coloring in the webapp (components.py:_get_color())
    FILE_CREATION_POKE_INTERVAL_S = 60

    ACQUISITION_MONITOR_POKE_INTERVAL_S = 30

    QUANTING_MONITOR_POKE_INTERVAL_S = 60

    FILE_COPY_TIMEOUT_M = 8

    ACQUISITION_MONITOR_TIMEOUT_M = 180


class Concurrency:
    """Concurrency constants."""

    # limit to a number smaller than maximum number of runs per DAG (default is 16) to have free slots for other tasks
    # like starting quanting or metrics calculation
    MAX_ACTIVE_QUANTING_MONITORINGS_PER_DAG = 14

    # limit the number of concurrent copies to not over-stress the network.
    # Note that this is a potential bottleneck, so a timeout is important here.
    MAX_ACTIVE_COPY_TASKS_PER_DAG = 1

    # limit the number of concurrent monitors to not over-stress the network (relevant only during a catchup)
    MAX_MONITOR_ACQUISITION_TASKS_PER_DAG = 5


class Pools:
    """Pool names.

    cf. https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html
    """

    # pool to limit file copying across all instruments
    FILE_COPY_POOL = "file_copy_pool"  # suggested default: 2


def get_internal_instrument_data_path(instrument_id: str) -> Path:
    """Get internal path for the given instrument.

    e.g. /opt/airflow/mounts/instruments/test2
    """
    return Path(InternalPaths.MOUNTS_PATH) / InternalPaths.INSTRUMENTS / instrument_id


def get_internal_backup_path() -> Path:
    """Get internal backup path.

    e.g. /opt/airflow/mounts/backup
    """
    return Path(InternalPaths.MOUNTS_PATH) / InternalPaths.BACKUP


def get_internal_instrument_backup_path(instrument_id: str) -> Path:
    """Get internal path for the given instrument.

    e.g. /opt/airflow/mounts/backup/test2
    """
    return get_internal_backup_path() / instrument_id


def get_output_folder_rel_path(raw_file_name: str, project_id: str) -> Path:
    """Get the relative path of the output directory for given raw file name."""
    return (
        Path(InternalPaths.OUTPUT)
        / project_id
        / f"{OUTPUT_FOLDER_PREFIX}{raw_file_name}"
    )


def get_internal_output_path(raw_file_name: str, project_id: str) -> Path:
    """Get absolute internal output path for the given raw file name."""
    return Path(InternalPaths.MOUNTS_PATH) / get_output_folder_rel_path(
        raw_file_name, project_id
    )


def get_instrument_type(instrument_id: str) -> str:
    """Get the type of the instrument with the given ID."""
    return INSTRUMENTS[instrument_id][InstrumentKeys.TYPE]


def get_fallback_project_id(instrument_id: str) -> str:
    """Get the fallback project id."""
    # This is on the edge of being hacky, this information could also be included in the `INSTRUMENTS` dict.
    return (
        FALLBACK_PROJECT_ID_BRUKER
        if INSTRUMENTS[instrument_id][InstrumentKeys.TYPE] == InstrumentTypes.BRUKER
        else FALLBACK_PROJECT_ID
    )
