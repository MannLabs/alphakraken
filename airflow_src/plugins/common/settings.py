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

CLUSTER_BASE_DIR = "~/slurm"
CLUSTER_JOB_SCRIPT_PATH = f"{CLUSTER_BASE_DIR}/submit_job.sh"
CLUSTER_WORKING_DIR = f"{CLUSTER_BASE_DIR}/jobs"

FALLBACK_PROJECT_ID = "_FALLBACK"
FALLBACK_PROJECT_ID_BRUKER = "_FALLBACK_BRUKER"

OUTPUT_FOLDER_PREFIX = "out_"

# local folder on the instruments to move files to after copying to pool-backup
INSTRUMENT_BACKUP_FOLDER_NAME = "Backup"

# separator between the timestamp and the raw file id in case of collisions
COLLISION_FLAG_SEP = "---"

# relevant for Bruker only
DEFAULT_RAW_FILE_SIZE_IF_MAIN_FILE_MISSING = -1

ERROR_CODE_TO_STRING = {
    "_CANNOT_FIND_ITEM": "Cannot find item [Idx] within the current storage",
    "_FAILED_TO_DETERMINE_DIA_CYCLE": "Failed to determine start of DIA cycle",
    "_RUN_HEADER_EX": "RunHeaderEx",
    "_ARRAY_ERROR": "array must not contain infs or NaNs",
    "_FRAGMENT_MZ_TOLERANCE_MUST_BE_LESS_THAN": "fragment_mz_tolerance must be less than",
    "_PRECURSOR_MZ_TOLERANCE_MUST_BE_LESS_THAN": "precursor_mz_tolerance must be less than",
    "_NEED_AT_LEAST_ONE_ARRAY": "need at least one array to concatenate",
    "_TRAIN_SET_WILL_BE_EMPTY": "the resulting train set will be empty",
    "_CYCLE_NOT_CONSISTENT": "but does not consistent",
    "_NO_PSM_FILES": "No psm files accumulated",  # will become a known error in alphadia >1.7.2
    "_NOT_DIA_DATA": "'TimsTOFTranspose' object has no attribute '_cycle'",  # will become a known error in alphadia >1.7.2
}


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

    RAW_DATA_COPY_TASK_TIMEOUT_M = 12

    # this timeout needs to be big compared to the time scales defined in AcquisitionMonitor
    ACQUISITION_MONITOR_TIMEOUT_M = 180

    MOVE_RAW_FILE_TASK_TIMEOUT_M = 5


class Concurrency:
    """Concurrency constants."""

    # limit to a number smaller than maximum number of runs per DAG (default is 16) to have free slots for other tasks
    # like starting quanting or metrics calculation
    MAXNO_MONITOR_QUANTING_TASKS_PER_DAG = 14

    # limit the number of concurrent copies to not over-stress the network.
    # Note that this is a potential bottleneck, so a timeout is important here.
    MAXNO_COPY_RAW_FILE_TASKS_PER_DAG = 1

    # limit the number of concurrent monitors to not over-stress the network (relevant only during a catchup)
    MAXNO_MONITOR_ACQUISITION_TASKS_PER_DAG = 10

    MAXNO_MOVE_RAW_FILE_TASKS_PER_DAG = 1


class Pools:
    """Pool names.

    cf. https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html
    """

    # pool to limit file copying across all instruments
    FILE_COPY_POOL = "file_copy_pool"  # suggested default: 3

    # pool to limit the number of concurrent jobs on the cluster
    CLUSTER_SLOTS_POOL = "cluster_slots_pool"  #  suggested default: 30


class AlphaDiaConstants:
    """Constants for accessing AlphaDia output."""

    LOG_FILE_NAME = "log.txt"
    EVENTS_FILE_NAME = "events.jsonl"
    PROGRESS_FOLDER_NAME = ".progress"


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
