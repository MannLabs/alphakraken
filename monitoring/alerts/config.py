"""Configuration for the monitoring service."""

import logging
import os
import sys
from collections import defaultdict

from shared.db.models import (
    InstrumentFileStatus,
)
from shared.keys import EnvVars

# Constants
CHECK_INTERVAL_SECONDS = 60
ALERT_COOLDOWN_MINUTES = (
    120  # Minimum time between repeated alerts for the same issue_type
)

STALE_STATUS_THRESHOLD_MINUTES = (
    15  # How old a kraken status can be before considered stale
)
FILE_REMOVER_STALE_THRESHOLD_HOURS = (
    24  # How long file_remover job can go without running
)
FREE_SPACE_THRESHOLD_GB = (
    200  # regardless of the configuration in airflow: 200 GB is very low
)
BACKUP_FREE_SPACE_THRESHOLD_GB = 500  # Backup filesystem threshold
OUTPUT_FREE_SPACE_THRESHOLD_GB = 300  # Output filesystem threshold

STATUS_PILE_UP_THRESHOLDS = defaultdict(lambda: 5)
STATUS_PILE_UP_THRESHOLDS["quanting"] = 10

# Instrument file pile-up thresholds
INSTRUMENT_FILE_PILE_UP_THRESHOLDS = {
    InstrumentFileStatus.NEW: 30,  # => indicates something is wrong with file moving
    # InstrumentFileStatus.MOVED: 50,  # doesn't make sense as the number depends on configured free disk space
}
INSTRUMENT_FILE_MIN_AGE_HOURS = 6  # Only consider files older than 6 hours

# Pump pressure alert configuration
PUMP_PRESSURE_LOOKBACK_DAYS = 7
PUMP_PRESSURE_WINDOW_SIZE = 5  # Number of samples to compare
PUMP_PRESSURE_THRESHOLD_BAR = 20  # Pressure increase threshold in bar
PUMP_PRESSURE_GRADIENT_TOLERANCE = 0.1

MESSENGER_WEBHOOK_URL: str = os.environ.get(EnvVars.MESSENGER_WEBHOOK_URL, "")
if not MESSENGER_WEBHOOK_URL:
    logging.error(f"{EnvVars.MESSENGER_WEBHOOK_URL} environment variable must be set")
    sys.exit(1)


class Cases:
    """Cases for which to send alerts."""

    STALE = "stale"
    LOW_DISK_SPACE = "low_disk_space"
    HEALTH_CHECK_FAILED = "health_check_failed"
    STATUS_PILE_UP = "status_pile_up"
    INSTRUMENT_FILE_PILE_UP = "instrument_file_pile_up"
    RAW_FILE_ERROR = "raw_file_error"
    WEBAPP_HEALTH = "webapp_health"
    PUMP_PRESSURE_INCREASE = "pump_pressure_increase"
