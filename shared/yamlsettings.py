"""Module to load and access the alphakraken.yaml settings."""

import logging
import os
from pathlib import Path
from typing import Any

import yaml

from shared.keys import EnvVars, InternalPaths


class YamlSettings:
    """Class to load and access the alphakraken.yaml settings as a singleton."""

    _instance: dict[str, dict[str, Any]] | None = None

    def __new__(cls) -> dict[str, dict[str, Any]]:
        """Get a new or existing instance of the YamlSettings class."""
        if cls._instance is None:
            cls._instance: dict[str, dict[str, Any]] = cls.load_alphakraken_yaml()
        return cls._instance.copy()

    @classmethod
    def load_alphakraken_yaml(cls) -> dict[str, dict[str, Any]]:
        """Load alphakraken settings from a YAML file."""
        env_name = os.getenv(EnvVars.ENV_NAME)

        file_name = f"alphakraken.{env_name}.yaml"
        file_path = Path(InternalPaths.ENVS_PATH) / file_name
        if env_name == "_test_":
            # TODO: this is to make the tests happy, but it should be handled differently
            logging.warning("Using 'test' environment, this is an error in production!")
            return {"instruments": {"_test1_": {"type": "thermo"}}}

        if not file_path.exists():
            raise FileNotFoundError(
                f"Settings file {file_name} not found at {file_path}"
            )

        with file_path.open() as file:
            logging.info(f"Loading settings from {file_name}")
            return yaml.safe_load(file)


YAMLSETTINGS = YamlSettings()


def get_path(path_key: str) -> Path:
    """Get a certain path from the yaml settings."""
    path = YAMLSETTINGS.get("locations", {}).get(path_key, {}).get("absolute_path")

    if path is None:
        raise KeyError(
            f"Key `{path_key}` or `{path_key}.absolute_path` not found in alphakraken.yaml."
        )

    return Path(path)
