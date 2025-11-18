"""Module to load and access the alphakraken.yaml settings."""

import logging
import os
from pathlib import Path
from typing import Any, cast

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

    NOTIFICATIONS = "notifications"
    OPS_ALERTS_WEBHOOK_URL = "ops_alerts_webhook_url"
    BUSINESS_ALERTS_WEBHOOK_URL = "business_alerts_webhook_url"
    HOSTNAME = "hostname"
    WEBAPP_URL = "webapp_url"

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
                    "notifications": {
                        "ops_alerts_webhook_url": "http://test-webhook.example.com",
                        "business_alerts_webhook_url": "http://test-webhook.example.com",
                        "hostname": "localhost",
                        "webapp_url": "http://localhost:8501",
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


YAMLSETTINGS: dict[str, dict[str, Any]] = cast(
    dict[str, dict[str, Any]], YamlSettings()
)


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


def get_notification_setting(setting_key: str) -> str:
    """Get a notification setting from the yaml settings."""
    setting_value = (
        YAMLSETTINGS.get(YamlKeys.GENERAL, {})
        .get(YamlKeys.NOTIFICATIONS, {})
        .get(setting_key)
    )

    if setting_value is None:
        raise KeyError(
            f"Key `{YamlKeys.GENERAL}.{YamlKeys.NOTIFICATIONS}.{setting_key}` not found in alphakraken.yaml."
        )

    return setting_value
