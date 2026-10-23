"""Script to monitor AlphaKraken and send alerts to Slack or MS Teams."""
# ruff: noqa: E402

import logging

# needs to be before other imports to setup logging:
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

import os
from datetime import datetime, timedelta
from time import sleep
from uuid import uuid4

import pytz
from active_instance import ActiveInstanceElection
from alert_manager import AlertManager
from alerts.config import (
    ACTIVE_INSTANCE_TIMEOUT_SECONDS,
    CHECK_INTERVAL_SECONDS,
    SPECIAL_ALERT_COOLDOWN_MINUTES,
    STALE_STATUS_THRESHOLD_MINUTES,
)
from messenger import AlertTypes, Message
from pymongo.errors import ConnectionFailure
from utils import extract_error_line

from shared.db.engine import connect_db
from shared.keys import EnvVars

# TODO: report when error has resolved
# TODO: add a "all is well" message once a day/week?
# TODO: health check if webapp is reachable


def main() -> None:
    """Main monitoring loop."""
    logging.info(
        f"Starting KrakenStatus monitor (check interval: {CHECK_INTERVAL_SECONDS}s, "
        f"stale threshold: {STALE_STATUS_THRESHOLD_MINUTES}m)"
    )

    alert_manager = AlertManager()
    # unique per instance, also if several instances run on the same host
    instance = f"{os.environ.get(EnvVars.KRAKEN_HOSTNAME, '')}-{uuid4().hex[:8]}"
    election = ActiveInstanceElection(
        instance, timedelta(seconds=ACTIVE_INSTANCE_TIMEOUT_SECONDS)
    )
    # sent by every instance, to see which ones are running
    alert_manager.send_once(
        "startup",
        Message("Monitoring started.", type=AlertTypes.INFO),
        SPECIAL_ALERT_COOLDOWN_MINUTES,
        datetime.now(pytz.UTC),
    )

    while True:
        now = datetime.now(pytz.UTC)
        # separate from the DB part, so that a failing DB-free check cannot stop the DB checks
        try:
            alert_manager.check_db_free_alerts(now)
        except Exception as e:  # noqa: BLE001
            _report_exception(alert_manager, e, now)

        try:
            connect_db(raise_on_error=True)
            if election.try_become_active(now):
                alert_manager.check_for_issues(now)
        except ConnectionFailure as e:
            alert_manager.send_once(
                "db_connection_error",
                Message(f"Error connecting to MongoDB: {e}"),
                SPECIAL_ALERT_COOLDOWN_MINUTES,
                now,
            )
        except Exception as e:  # noqa: BLE001
            _report_exception(alert_manager, e, now)

        sleep(CHECK_INTERVAL_SECONDS)


def _report_exception(alert_manager: AlertManager, e: Exception, now: datetime) -> None:
    """Log an unexpected exception and tell ops once per cooldown."""
    logging.exception("Error checking for issues")
    alert_manager.send_once(
        "exception",
        Message(
            f"{type(e).__name__} during checking alerts: {e} at: {extract_error_line(e)}"
        ),
        SPECIAL_ALERT_COOLDOWN_MINUTES,
        now,
    )


if __name__ == "__main__":
    main()
