"""Module to access the alphakraken.yaml file."""

import logging
from pathlib import Path
from typing import Any

import yaml
from common.constants import INSTRUMENT_SETTINGS_DEFAULTS, InternalPaths
from common.utils import get_env_variable

from shared.keys import EnvVars


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
    return settings_with_defaults[key]
