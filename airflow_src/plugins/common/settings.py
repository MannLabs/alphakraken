"""Keys for accessing Dags, Tasks, etc.."""

from pathlib import Path

from common.keys import InstrumentKeys
from common.utils import get_env_variable

INSTRUMENTS = {
    # the toplevel keys determine the DAG name (e.g. 'acquisition_watcher.test1')
    "test1": {
        # this variable must be defined in all .env files and be made available to the container in docker-compose.yml
        InstrumentKeys.RAW_DATA_PATH_VARIABLE_NAME: "INSTRUMENT_PATH_TEST1"
    },
    "test2": {InstrumentKeys.RAW_DATA_PATH_VARIABLE_NAME: "INSTRUMENT_PATH_ASTRAL1"},
    "test3": {InstrumentKeys.RAW_DATA_PATH_VARIABLE_NAME: "INSTRUMENT_PATH_ASTRAL2"},
    "test4": {InstrumentKeys.RAW_DATA_PATH_VARIABLE_NAME: "INSTRUMENT_PATH_ASTRAL3"},
}

# prefix for the queues the DAGs are assigned to (cf. docker-compose.yml)
AIRFLOW_QUEUE_PREFIX = "kraken_queue_"

OUTPUT_FOLDER_PREFIX = "out_"


CLUSTER_BASE_DIR = "~/slurm"
CLUSTER_JOB_SCRIPT_PATH = f"{CLUSTER_BASE_DIR}/submit_job.sh"
CLUSTER_WORKING_DIR = f"{CLUSTER_BASE_DIR}/jobs"

FALLBACK_PROJECT_ID = "_FALLBACK"


class InternalPaths:
    """Paths to directories within the Docker containers."""

    MOUNTS_PATH = "/opt/airflow/mounts/"
    INSTRUMENTS = "instruments"
    BACKUP = "backup"
    OUTPUT = "output"


class Timings:
    """Timing constants."""

    FILE_CREATION_POKE_INTERVAL_S = 60

    ACQUISITION_MONITOR_POKE_INTERVAL_S = 30

    QUANTING_MONITOR_POKE_INTERVAL_S = 60


class Concurrency:
    """Concurrency constants."""

    # limit to a number smaller than maximum number of runs per DAG (default is 16) to have free slots for other tasks
    # like starting quanting or metrics calculation
    MAX_ACTIVE_MONITORINGS_PER_DAG = 14


def get_internal_instrument_data_path(instrument_id: str) -> Path:
    """Get internal path for the given instrument.

    e.g. /opt/airflow/mounts/pool-backup/Test2
    """
    return Path(InternalPaths.MOUNTS_PATH) / get_relative_instrument_data_path(
        instrument_id
    )


def get_relative_instrument_data_path(instrument_id: str) -> str:
    """Get relative_path for the given instrument.

    e.g. pool-backup/Test2
    """
    # TODO: this name could be derived from the instrument_id by convention, but then it's less explicit
    raw_data_path_variable_name = INSTRUMENTS[instrument_id][
        InstrumentKeys.RAW_DATA_PATH_VARIABLE_NAME
    ]

    # TODO: fix: we need to provide a default here to make test_dags.py happy. Should not be an issue in production
    # as the variable is enforced to be set in docker-compose.yml
    return get_env_variable(
        raw_data_path_variable_name, "error_raw_data_path_variable_name_not_set"
    )


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
