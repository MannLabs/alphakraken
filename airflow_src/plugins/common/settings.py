"""Module to access constant and dynamic settings (given by the alphakraken.yaml file)."""

import logging
from pathlib import Path
from typing import Any

import yaml
from common.constants import InternalPaths
from common.keys import InstrumentKeys, InstrumentTypes
from common.utils import get_env_variable

from shared.keys import EnvVars


class Timings:
    """Timing constants."""

    # if you update this, you might also want to update the coloring in the webapp (components.py:_get_color())
    FILE_SENSOR_POKE_INTERVAL_S: int = 60

    ACQUISITION_MONITOR_POKE_INTERVAL_S: int = 30

    QUANTING_MONITOR_POKE_INTERVAL_S: int = 60

    RAW_DATA_COPY_TASK_TIMEOUT_M: int = 15  # large enough to not time out on big files, small enough to not block other tasks

    # this timeout needs to be big compared to the time scales defined in AcquisitionMonitor
    ACQUISITION_MONITOR_TIMEOUT_M: int = 180

    MOVE_RAW_FILE_TASK_TIMEOUT_M: int = 5

    REMOVE_RAW_FILE_TASK_TIMEOUT_M: int = 6 * 60  # runs long due to hashsum calculation

    DEFAULT_FILE_MOVE_DELAY_M: int = 5

    FILE_MOVE_RETRY_DELAY_M: int = 30


class Concurrency:
    """Concurrency constants."""

    # limit to a number smaller than maximum number of runs per DAG (default is 16) to have free slots for other tasks
    # like starting quanting or metrics calculation
    MAXNO_MONITOR_QUANTING_TASKS_PER_DAG: int = 14

    # limit the number of concurrent copies to not over-stress the network.
    # Note that this is a potential bottleneck, so a timeout is important here.
    MAXNO_COPY_RAW_FILE_TASKS_PER_DAG: int = 2

    # limit the number of concurrent monitors to not over-stress the network (relevant only during a catchup)
    MAXNO_MONITOR_ACQUISITION_TASKS_PER_DAG: int = 10

    MAXNO_MOVE_RAW_FILE_TASKS_PER_DAG: int = 1


INSTRUMENT_SETTINGS_DEFAULTS = {
    InstrumentKeys.SKIP_QUANTING: False,
    InstrumentKeys.MIN_FREE_SPACE_GB: None,  # None -> use value from Airflow variable MIN_FREE_SPACE_GB
    InstrumentKeys.FILE_MOVE_DELAY_M: Timings.DEFAULT_FILE_MOVE_DELAY_M,
}

# local folder on the instruments to move files to after copying to pool-backup
INSTRUMENT_BACKUP_FOLDER_NAME = "Backup"  # TODO: rename this folder to "handled" or similar to avoid confusion with pool backup

DEFAULT_MIN_FILE_AGE_TO_REMOVE_D = 14  # days
# this is to avoid getting a lot of removal candidates:
DEFAULT_MAX_FILE_AGE_TO_REMOVE_D = 60  # days


# TODO: make this dynamic & symmetric
FALLBACK_PROJECT_ID = "_FALLBACK"
FALLBACK_PROJECT_ID_BRUKER = "_FALLBACK_BRUKER"


def get_fallback_project_id(instrument_id: str) -> str:
    """Get the fallback project id.

    Fallback project IDs are used to get the respective settings and the output
    folder in case no matching project ID is found.
    """
    # This is on the edge of being hacky, this information could also be included in the `INSTRUMENTS` dict.
    return (
        FALLBACK_PROJECT_ID_BRUKER
        if get_instrument_settings(instrument_id, InstrumentKeys.TYPE)
        == InstrumentTypes.BRUKER
        else FALLBACK_PROJECT_ID
    )


def _load_alphakraken_yaml(env_name: str) -> dict[str, dict[str, Any]]:
    """Load alphakraken settings from a YAML file."""
    file_name = f"alphakraken.{env_name}.yaml"
    file_path = Path(InternalPaths.ENVS_PATH) / file_name
    if env_name == "_test_":
        # TODO: this is to make the tests happy, but it should be handled differently
        logging.warning("Using 'test' environment, this is an error in production!")
        return {"instruments": {"_test1_": {"type": "thermo"}}}

    if not file_path.exists():
        raise FileNotFoundError(f"Settings file {file_name} not found at {file_path}")

    with file_path.open() as file:
        logging.info(f"Loading settings from {file_name}")
        return yaml.safe_load(file)


_SETTINGS = _load_alphakraken_yaml(
    get_env_variable(EnvVars.ENV_NAME, "none", verbose=False)
)
_INSTRUMENTS = _SETTINGS["instruments"].copy()


def get_instrument_ids() -> list[str]:
    """Get all IDs for all instruments."""
    return list(_INSTRUMENTS.keys())


def get_instrument_settings(instrument_id: str, key: str) -> Any:  # noqa: ANN401
    """Get a certain setting for an instrument."""
    settings_with_defaults = INSTRUMENT_SETTINGS_DEFAULTS | _INSTRUMENTS[instrument_id]
    if key not in settings_with_defaults:
        raise KeyError(
            f"Setting {key} not found for instrument {instrument_id}. {INSTRUMENT_SETTINGS_DEFAULTS=} {_INSTRUMENTS[instrument_id]=} "
        )
    return settings_with_defaults[key]
