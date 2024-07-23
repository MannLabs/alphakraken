"""Keys for accessing Dags, Tasks, etc.. and methods closely related."""

from pathlib import Path

from common.keys import InstrumentKeys, InstrumentTypes

from shared.db.models import RawFile, get_created_at_year_month

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

# separator between the timestamp and the raw file id in case of collisions
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
    FILE_SENSOR_POKE_INTERVAL_S = 60

    ACQUISITION_MONITOR_POKE_INTERVAL_S = 30

    QUANTING_MONITOR_POKE_INTERVAL_S = 60

    RAW_DATA_COPY_TASK_TIMEOUT_M = 8

    ACQUISITION_MONITOR_TIMEOUT_M = 180


class Concurrency:
    """Concurrency constants."""

    # limit to a number smaller than maximum number of runs per DAG (default is 16) to have free slots for other tasks
    # like starting quanting or metrics calculation
    MAXNO_MONITOR_QUANTING_TASKS_PER_DAG = 14

    # limit the number of concurrent copies to not over-stress the network.
    # Note that this is a potential bottleneck, so a timeout is important here.
    MAXNO_COPY_RAW_DATA_TASKS_PER_DAG = 1

    # limit the number of concurrent monitors to not over-stress the network (relevant only during a catchup)
    MAXNO_MONITOR_ACQUISITION_TASKS_PER_DAG = 10


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


def get_output_folder_rel_path(raw_file: RawFile, project_id_or_fallback: str) -> Path:
    """Get the relative path of the output directory for given raw file name.

    Only if the raw_file has no project defined, we use a month-specific subfolder
    This is to avoid having too many files in the fallback output folders.

    E.g.
        output/<project_id_or_fallback>>/2024_07/out_RAW-FILE-1.raw in case raw_file has no project ID
        output/<project_id_or_fallback>>/out_RAW-FILE-1.raw in case raw_file has a project ID
    """
    optional_sub_folder = (
        get_created_at_year_month(raw_file) if raw_file.project_id is None else ""
    )
    return (
        Path(InternalPaths.OUTPUT)
        / project_id_or_fallback
        / optional_sub_folder
        / f"{OUTPUT_FOLDER_PREFIX}{raw_file.id}"
    )


def get_internal_output_path(raw_file: RawFile, project_id_or_fallback: str) -> Path:
    """Get absolute internal output path for the given raw file name."""
    return Path(InternalPaths.MOUNTS_PATH) / get_output_folder_rel_path(
        raw_file, project_id_or_fallback
    )


def get_instrument_type(instrument_id: str) -> str:
    """Get the type of the instrument with the given ID."""
    return INSTRUMENTS[instrument_id][InstrumentKeys.TYPE]


def get_fallback_project_id(instrument_id: str) -> str:
    """Get the fallback project id.

    Fallback project IDs are used to get the respective settings and the output
    folder in case no matching project ID is found.
    """
    # This is on the edge of being hacky, this information could also be included in the `INSTRUMENTS` dict.
    return (
        FALLBACK_PROJECT_ID_BRUKER
        if INSTRUMENTS[instrument_id][InstrumentKeys.TYPE] == InstrumentTypes.BRUKER
        else FALLBACK_PROJECT_ID
    )
