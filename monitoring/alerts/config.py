"""Configuration for the monitoring service."""

import logging
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml

from shared.keys import EnvVars, InternalPaths
from shared.yamlsettings import YamlKeys, get_notification_setting


class AlertsYamlKeys:
    """Keys for accessing the alerts.yaml settings."""

    THRESHOLDS = "thresholds"
    FREE_SPACE_THRESHOLD_GB = "free_space_threshold_gb"
    FILE_SYSTEM_FREE_SPACE_THRESHOLDS_GB = "file_system_free_space_thresholds_gb"
    LOCAL_FREE_SPACE_THRESHOLD_GB = "local_free_space_threshold_gb"
    PUMP_PRESSURE_THRESHOLD_BAR = "pump_pressure_threshold_bar"
    PUMP_PRESSURE_ABSOLUTE_THRESHOLD_BAR = "pump_pressure_absolute_threshold_bar"
    STALE_STATUS_THRESHOLD_MINUTES = "stale_status_threshold_minutes"
    FILE_REMOVER_STALE_THRESHOLD_HOURS = "file_remover_stale_threshold_hours"
    INSTRUMENT_STALL_THRESHOLD_HOURS = "instrument_stall_threshold_hours"
    RAW_FILE_STATUS_LOOKBACK_HOURS = "raw_file_status_lookback_hours"
    STATUS_PILE_UP_THRESHOLDS = "status_pile_up_thresholds"
    INSTRUMENT_FILE_PILE_UP_THRESHOLDS = "instrument_file_pile_up_thresholds"
    INSTRUMENT_FILE_MIN_AGE_HOURS = "instrument_file_min_age_hours"
    # status_pile_up_thresholds: the threshold of the statuses not listed
    DEFAULT = "default"

    PUMP_PRESSURE = "pump_pressure"
    LOOKBACK_DAYS = "lookback_days"
    WINDOW_SIZE = "window_size"
    GRADIENT_TOLERANCE = "gradient_tolerance"

    QUEUE_STOP = "queue_stop"
    THRESHOLD_MULTIPLIER = "threshold_multiplier"
    MAX_GRADIENT_LENGTH_HOURS = "max_gradient_length_hours"
    MAX_FILE_AGE_HOURS = "max_file_age_hours"

    ALERTS = "alerts"
    DESTINATIONS = "destinations"
    COOLDOWN_MINUTES = "cooldown_minutes"
    FILE_REMOVER_COOLDOWN_MINUTES = "file_remover_cooldown_minutes"


ALERTS_YAML_PATH = Path(InternalPaths.ENVS_PATH) / "alerts.yaml"
if os.getenv(EnvVars.ENV_NAME) == "_test_":
    # tests run outside the container: read the repository's file
    ALERTS_YAML_PATH = Path(__file__).parents[2] / "envs" / "alerts.yaml"

with ALERTS_YAML_PATH.open() as _file:
    ALERTSETTINGS: dict[str, Any] = yaml.safe_load(_file)


def get_alert_setting(block: str, setting_key: str) -> Any:
    """Get a setting from a block of alerts.yaml."""
    try:
        return ALERTSETTINGS[block][setting_key]
    except KeyError as e:
        raise KeyError(f"Key `{block}.{setting_key}` not found in alerts.yaml.") from e


def _threshold(setting_key: str) -> Any:
    return get_alert_setting(AlertsYamlKeys.THRESHOLDS, setting_key)


def _cooldown(minutes: int) -> int | None:
    """Translate a cooldown of alerts.yaml to the internal one: None notifies only once."""
    return None if minutes == COOLDOWN_ONCE else minutes


# Constants
CHECK_INTERVAL_SECONDS = 60
# only the active instance runs the DB-backed checks; another takes over if it does not renew its claim within this time.
# The claim is renewed once per loop, so this must cover the checks plus the sleep with a margin
ACTIVE_INSTANCE_TIMEOUT_SECONDS = 10 * CHECK_INTERVAL_SECONDS
# cooldown of the alerts sent without DB access (startup, DB and check errors)
SPECIAL_ALERT_COOLDOWN_MINUTES = 10
# cooldown_minutes in alerts.yaml that notifies an issue only once
COOLDOWN_ONCE = -1

# bounds on channel messages, so that no message exceeds the webhook payload limits
MAX_ISSUES_PER_MESSAGE = 20
MAX_DETAILS_CHARS = 300

STALE_STATUS_THRESHOLD_MINUTES: int = _threshold(
    AlertsYamlKeys.STALE_STATUS_THRESHOLD_MINUTES
)
FILE_REMOVER_STALE_THRESHOLD_HOURS: int = _threshold(
    AlertsYamlKeys.FILE_REMOVER_STALE_THRESHOLD_HOURS
)
FREE_SPACE_THRESHOLD_GB: int = _threshold(AlertsYamlKeys.FREE_SPACE_THRESHOLD_GB)
# file systems with a threshold other than FREE_SPACE_THRESHOLD_GB
FILE_SYSTEM_FREE_SPACE_THRESHOLDS_GB: dict[str, int] = _threshold(
    AlertsYamlKeys.FILE_SYSTEM_FREE_SPACE_THRESHOLDS_GB
)

# Local disk alert configuration
BYTES_PER_GB = 1024**3
LOCAL_DISK_PATH = (
    "/etc/hosts"  # always-present path probing the monitoring host's local disk
)
LOCAL_FREE_SPACE_THRESHOLD_GB: int = _threshold(
    AlertsYamlKeys.LOCAL_FREE_SPACE_THRESHOLD_GB
)

_status_pile_up: dict[str, int] = _threshold(AlertsYamlKeys.STATUS_PILE_UP_THRESHOLDS)
STATUS_PILE_UP_THRESHOLDS: defaultdict[str, int] = defaultdict(
    lambda: _status_pile_up[AlertsYamlKeys.DEFAULT],
    {
        status: threshold
        for status, threshold in _status_pile_up.items()
        if status != AlertsYamlKeys.DEFAULT
    },
)
# keyed by InstrumentFileStatus
INSTRUMENT_FILE_PILE_UP_THRESHOLDS: dict[str, int] = _threshold(
    AlertsYamlKeys.INSTRUMENT_FILE_PILE_UP_THRESHOLDS
)
INSTRUMENT_FILE_MIN_AGE_HOURS: int = _threshold(
    AlertsYamlKeys.INSTRUMENT_FILE_MIN_AGE_HOURS
)
RAW_FILE_STATUS_LOOKBACK_HOURS: int = _threshold(
    AlertsYamlKeys.RAW_FILE_STATUS_LOOKBACK_HOURS
)
INSTRUMENT_STALL_THRESHOLD_HOURS: int = _threshold(
    AlertsYamlKeys.INSTRUMENT_STALL_THRESHOLD_HOURS
)

QUEUE_STOP_THRESHOLD_MULTIPLIER: int = get_alert_setting(
    AlertsYamlKeys.QUEUE_STOP, AlertsYamlKeys.THRESHOLD_MULTIPLIER
)
MAX_GRADIENT_LENGTH_HOURS: float = get_alert_setting(
    AlertsYamlKeys.QUEUE_STOP, AlertsYamlKeys.MAX_GRADIENT_LENGTH_HOURS
)
QUEUE_STOP_MAX_FILE_AGE_HOURS: int = get_alert_setting(
    AlertsYamlKeys.QUEUE_STOP, AlertsYamlKeys.MAX_FILE_AGE_HOURS
)

PUMP_PRESSURE_LOOKBACK_DAYS: int = get_alert_setting(
    AlertsYamlKeys.PUMP_PRESSURE, AlertsYamlKeys.LOOKBACK_DAYS
)
PUMP_PRESSURE_WINDOW_SIZE: int = get_alert_setting(
    AlertsYamlKeys.PUMP_PRESSURE, AlertsYamlKeys.WINDOW_SIZE
)
PUMP_PRESSURE_GRADIENT_TOLERANCE: float = get_alert_setting(
    AlertsYamlKeys.PUMP_PRESSURE, AlertsYamlKeys.GRADIENT_TOLERANCE
)
PUMP_PRESSURE_THRESHOLD_BAR: float = _threshold(
    AlertsYamlKeys.PUMP_PRESSURE_THRESHOLD_BAR
)
PUMP_PRESSURE_ABSOLUTE_THRESHOLD_BAR: float = _threshold(
    AlertsYamlKeys.PUMP_PRESSURE_ABSOLUTE_THRESHOLD_BAR
)

try:
    OPS_ALERTS_URL: str = get_notification_setting(YamlKeys.OPS_ALERTS_URL)
except KeyError:
    logging.exception("Failed to load ops alerts URL from config.")
    sys.exit(1)

try:
    BUSINESS_ALERTS_URL: str = get_notification_setting(YamlKeys.BUSINESS_ALERTS_URL)
except KeyError:
    logging.warning("Business alerts URL not found in config, using ops alerts URL")
    BUSINESS_ALERTS_URL = OPS_ALERTS_URL

# replaced by the messenger id of the user in the user alerts URL
MESSENGER_ID_PLACEHOLDER = "{{ALPHAKRAKEN_MESSENGER_ID}}"
try:
    USER_ALERTS_URL: str = get_notification_setting(YamlKeys.USER_ALERTS_URL)
except KeyError:
    logging.warning("User alerts URL not found in config; direct messages disabled.")
    USER_ALERTS_URL = ""
if USER_ALERTS_URL and MESSENGER_ID_PLACEHOLDER not in USER_ALERTS_URL:
    logging.error(f"User alerts URL must contain {MESSENGER_ID_PLACEHOLDER}.")
    sys.exit(1)
# optional: messenger id that gets a copy of every direct message
DEBUG_MESSENGER_ID: str = get_notification_setting(YamlKeys.DEBUG_MESSENGER_ID, "")

try:
    INSTRUMENT_STALL_INSTRUMENT_IDS: set[str] = set(
        get_notification_setting(YamlKeys.INSTRUMENT_STALL_INSTRUMENT_IDS)
    )
except KeyError:
    logging.warning(
        "Instrument stall instrument ids not found in config; instrument stall alert disabled."
    )
    INSTRUMENT_STALL_INSTRUMENT_IDS = set()


class Destinations:
    """Where alerts can be sent to."""

    # DM to the owner of the project of the issue's relevant file, else to the user with the initials in the file name
    USER = "user"
    OPS = "ops"
    BUSINESS = "business"

    ALL = frozenset({USER, OPS, BUSINESS})


_alerts: dict[str, dict[str, Any]] = ALERTSETTINGS[AlertsYamlKeys.ALERTS]

# alert class name -> destinations of its issues; none for the DB-free alerts, they always go to ops
ALERT_DESTINATIONS: dict[str, frozenset[str]] = {
    alert_name: frozenset(settings[AlertsYamlKeys.DESTINATIONS])
    for alert_name, settings in _alerts.items()
    if AlertsYamlKeys.DESTINATIONS in settings
}
for _alert_name, _destinations in ALERT_DESTINATIONS.items():
    if unknown := _destinations - Destinations.ALL:
        logging.error(
            f"Unknown destinations {sorted(unknown)} for {_alert_name}; allowed: {sorted(Destinations.ALL)}."
        )
        sys.exit(1)

# alert class name -> minimum minutes between notifications of the same issue, None: only once
ALERT_COOLDOWNS_MINUTES: dict[str, int | None] = {
    alert_name: _cooldown(settings[AlertsYamlKeys.COOLDOWN_MINUTES])
    for alert_name, settings in _alerts.items()
}
FILE_REMOVER_HEALTH_COOLDOWN_MINUTES: int | None = _cooldown(
    _alerts["HealthCheckFailedAlert"][AlertsYamlKeys.FILE_REMOVER_COOLDOWN_MINUTES]
)
