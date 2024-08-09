"""Keys for accessing Dags, Tasks, etc.."""

from pathlib import Path

from common.keys import InstrumentKeys
from common.utils import get_env_variable

from shared.keys import EnvVars

INSTRUMENTS = {
    # the toplevel keys determine the DAG name (e.g. 'acquisition_watcher.test6')
    "test1": {
        # raw data path relative to InternalPaths.MOUNTS_PATH which must be mounted in docker-compose.yml
        # TODO: fix: we need to provide a default here to make test_dags.py happy. Should not be an issue in production.
        InstrumentKeys.RAW_DATA_PATH: get_env_variable(
            EnvVars.INSTRUMENT_PATH_TEST1, "n_a"
        ),
    },
    "test2": {
        InstrumentKeys.RAW_DATA_PATH: get_env_variable(
            EnvVars.INSTRUMENT_PATH_ASTRAL1, "n_a"
        ),
    },
    "test3": {
        InstrumentKeys.RAW_DATA_PATH: get_env_variable(
            EnvVars.INSTRUMENT_PATH_ASTRAL2, "n_a"
        ),
    },
    "test4": {
        InstrumentKeys.RAW_DATA_PATH: get_env_variable(
            EnvVars.INSTRUMENT_PATH_ASTRAL3, "n_a"
        ),
    },
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
    OUTPUT = "output"


class Timings:
    """Timing constants."""

    FILE_CREATION_POKE_INTERVAL_S = 60

    QUANTING_MONITOR_POKE_INTERVAL_S = 60


def get_internal_instrument_data_path(instrument_id: str) -> Path:
    """Get internal path for the given instrument.

    e.g. /opt/airflow/mounts/pool-backup/Test2
    """
    return (
        Path(InternalPaths.MOUNTS_PATH)
        / INSTRUMENTS[instrument_id][InstrumentKeys.RAW_DATA_PATH]
    )


def get_relative_instrument_data_path(instrument_id: str) -> str:
    """Get relative_path for the given instrument.

    e.g. pool-backup/Test2
    """
    return INSTRUMENTS[instrument_id][InstrumentKeys.RAW_DATA_PATH]


def get_output_folder_rel_path(raw_file_name: str, project_id: str) -> Path:
    """Get the relative path of the output directory for given raw file name."""
    return (
        Path(InternalPaths.OUTPUT)
        / project_id
        / f"{OUTPUT_FOLDER_PREFIX}{raw_file_name}"
    )


def get_internal_output_path(raw_file_name: str, project_id: str) -> Path:
    """Get abolute internal output path for the given raw file name."""
    return Path(InternalPaths.MOUNTS_PATH) / get_output_folder_rel_path(
        raw_file_name, project_id
    )
