"""Configuration for the monitoring service."""

import logging
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import yaml

from shared.db.models import (
    InstrumentFileStatus,
)
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

    DESTINATIONS = "destinations"


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


# Constants
CHECK_INTERVAL_SECONDS = 60
# only the active instance runs the DB-backed checks; another takes over if it does not renew its claim within this time.
# The claim is renewed once per loop, so this must cover the checks plus the sleep with a margin
ACTIVE_INSTANCE_TIMEOUT_SECONDS = 10 * CHECK_INTERVAL_SECONDS
DEFAULT_ALERT_COOLDOWN_TIME_MINUTES = (
    120  # Minimum time between repeated alerts for the same issue_type
)
# cooldown of the alerts sent without DB access (startup, DB and check errors)
SPECIAL_ALERT_COOLDOWN_MINUTES = 10

# bounds on channel messages, so that no message exceeds the webhook payload limits
MAX_ISSUES_PER_MESSAGE = 20
MAX_DETAILS_CHARS = 300

STALE_STATUS_THRESHOLD_MINUTES = (
    15  # How old a kraken status can be before considered stale
)
FILE_REMOVER_STALE_THRESHOLD_HOURS = (
    24  # How long file_remover job can go without running
)
# a failed file remover run is reported less often, it runs rarely
FILE_REMOVER_HEALTH_COOLDOWN_MINUTES = 12 * 60
FREE_SPACE_THRESHOLD_GB: int = get_alert_setting(
    AlertsYamlKeys.THRESHOLDS, AlertsYamlKeys.FREE_SPACE_THRESHOLD_GB
)
# file systems with a threshold other than FREE_SPACE_THRESHOLD_GB
FILE_SYSTEM_FREE_SPACE_THRESHOLDS_GB: dict[str, int] = get_alert_setting(
    AlertsYamlKeys.THRESHOLDS, AlertsYamlKeys.FILE_SYSTEM_FREE_SPACE_THRESHOLDS_GB
)

# Local disk alert configuration
BYTES_PER_GB = 1024**3
LOCAL_DISK_PATH = (
    "/etc/hosts"  # always-present path probing the monitoring host's local disk
)
# Local disk of the monitoring host
LOCAL_FREE_SPACE_THRESHOLD_GB: int = get_alert_setting(
    AlertsYamlKeys.THRESHOLDS, AlertsYamlKeys.LOCAL_FREE_SPACE_THRESHOLD_GB
)

STATUS_PILE_UP_THRESHOLDS = defaultdict(lambda: 5)
STATUS_PILE_UP_THRESHOLDS["quanting"] = 10

# Instrument file pile-up thresholds
INSTRUMENT_FILE_PILE_UP_THRESHOLDS = {
    InstrumentFileStatus.INITIAL: 30,  # => indicates something is wrong with file moving
    # InstrumentFileStatus.MOVED: 50,  # doesn't make sense as the number depends on configured free disk space
}
INSTRUMENT_FILE_MIN_AGE_HOURS = 6  # Only consider files older than 6 hours for pile up

# Raw file error / S3 upload failure alerts: only consider files updated within this time
RAW_FILE_STATUS_LOOKBACK_HOURS = 24

# Instrument stall alert configuration
INSTRUMENT_STALL_THRESHOLD_HOURS = 2  # How long without a new file before alerting

# Queue-stop alert configuration
QUEUE_STOP_THRESHOLD_MULTIPLIER = 3  # stop: pause > N x gradient_length triggers alert
MAX_GRADIENT_LENGTH_HOURS = 2.5  # gap larger than this => new queue, no alert
# files older than this are ignored: a queue that ended long ago is not worth a notification
QUEUE_STOP_MAX_FILE_AGE_HOURS = 24

# Pump pressure alert configuration
PUMP_PRESSURE_LOOKBACK_DAYS = 1
PUMP_PRESSURE_WINDOW_SIZE = 5  # Number of samples to compare
# Pressure increase threshold in bar
PUMP_PRESSURE_THRESHOLD_BAR: float = get_alert_setting(
    AlertsYamlKeys.THRESHOLDS, AlertsYamlKeys.PUMP_PRESSURE_THRESHOLD_BAR
)
# Absolute pressure threshold in bar
PUMP_PRESSURE_ABSOLUTE_THRESHOLD_BAR: float = get_alert_setting(
    AlertsYamlKeys.THRESHOLDS, AlertsYamlKeys.PUMP_PRESSURE_ABSOLUTE_THRESHOLD_BAR
)
PUMP_PRESSURE_GRADIENT_TOLERANCE = 0.1

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


# alert class name -> destinations of its issues
ALERT_DESTINATIONS: dict[str, frozenset[str]] = {
    alert_name: frozenset(destinations)
    for alert_name, destinations in ALERTSETTINGS[AlertsYamlKeys.DESTINATIONS].items()
}
for _alert_name, _destinations in ALERT_DESTINATIONS.items():
    if unknown := _destinations - Destinations.ALL:
        logging.error(
            f"Unknown destinations {sorted(unknown)} for {_alert_name}; allowed: {sorted(Destinations.ALL)}."
        )
        sys.exit(1)
