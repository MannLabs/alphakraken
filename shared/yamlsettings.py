"""Module to load and access the alphakraken.yaml settings."""

import logging
import os
from pathlib import Path
from typing import Any

import yaml

from shared.keys import EnvVars, InternalPaths


class YamlKeys:
    """Keys for accessing the alphakraken.yaml settings."""

    GENERAL = "general"
    INSTRUMENTS = "instruments"

    JOB_ENGINE = "job_engine"
    TYPE = "type"

    LOCATIONS = "locations"
    ABSOLUTE_PATH = "absolute_path"

    WEBHOOK_URLS = "webhook_urls"
    OPS_ALERTS = "ops_alerts"
    BUSINESS_ALERTS = "business_alerts"

    class Locations:
        """Keys for accessing paths in the yaml config."""

        BACKUP = "backup"
        SETTINGS = "settings"
        OUTPUT = "output"
        SLURM = "slurm"
        SOFTWARE = "software"


class YamlSettings:
    """Class to load and access the alphakraken.yaml settings as a singleton."""

    _instance: dict[str, dict[str, Any]] | None = None

    def __new__(cls) -> dict[str, dict[str, Any]]:
        """Get a new or existing instance of the YamlSettings class."""
        if cls._instance is None:
            cls._instance = cls.load_alphakraken_yaml()
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
            return {
                "instruments": {"_test1_": {"type": "thermo"}},
                "general": {
                    "webhook_urls": {
                        "ops_alerts": "http://test-webhook.example.com",
                        "business_alerts": "http://test-webhook.example.com",
                    }
                },
            }

        if not file_path.exists():
            raise FileNotFoundError(
                f"Settings file {file_name} not found at {file_path}"
            )

        with file_path.open() as file:
            logging.info(f"Loading settings from {file_path}")
            return yaml.safe_load(file)


YAMLSETTINGS = YamlSettings()


def get_path(path_key: str) -> Path:
    """Get a certain path from the yaml settings."""
    path = (
        YAMLSETTINGS.get(YamlKeys.LOCATIONS, {})
        .get(path_key, {})
        .get(YamlKeys.ABSOLUTE_PATH)
    )

    if path is None:
        raise KeyError(
            f"Key `{YamlKeys.LOCATIONS}.{path_key}` or `{YamlKeys.LOCATIONS}.{path_key}.{YamlKeys.ABSOLUTE_PATH}` not found in alphakraken.yaml."
        )

    return Path(path)


def get_webhook_url(webhook_key: str) -> str:
    """Get a webhook URL from the yaml settings."""
    webhook_url = (
        YAMLSETTINGS.get(YamlKeys.GENERAL, {})
        .get(YamlKeys.WEBHOOK_URLS, {})
        .get(webhook_key)
    )

    if webhook_url is None:
        raise KeyError(
            f"Key `{YamlKeys.GENERAL}.{YamlKeys.WEBHOOK_URLS}.{webhook_key}` not found in alphakraken.yaml."
        )

    return webhook_url
