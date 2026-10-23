"""Script to monitor AlphaKraken and send alerts to Slack or MS Teams."""
# ruff: noqa: E402

import logging

# needs to be before other imports to setup logging:
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

from datetime import datetime
from time import sleep

import pytz
from alert_manager import AlertManager
from alerts.config import (
    CHECK_INTERVAL_SECONDS,
    SPECIAL_ALERT_COOLDOWN_MINUTES,
    STALE_STATUS_THRESHOLD_MINUTES,
)
from messenger_clients import AlertTypes, Message
from pymongo.errors import ConnectionFailure
from utils import extract_error_line

from shared.db.engine import connect_db

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
    alert_manager.send_once(
        "startup",
        Message("Monitoring started.", type=AlertTypes.INFO),
        SPECIAL_ALERT_COOLDOWN_MINUTES,
        datetime.now(pytz.UTC),
    )

    while True:
        now = datetime.now(pytz.UTC)
        try:
            alert_manager.check_db_free_alerts(now)
            connect_db(raise_on_error=True)
            alert_manager.check_for_issues(now)
        except ConnectionFailure as e:
            alert_manager.send_once(
                "db_connection_error",
                Message(f"Error connecting to MongoDB: {e}"),
                SPECIAL_ALERT_COOLDOWN_MINUTES,
                now,
            )
        except Exception as e:
            logging.exception("Error checking for issues")
            alert_manager.send_once(
                "exception",
                Message(
                    f"{type(e).__name__} during checking alerts: {e} at: {extract_error_line(e)}"
                ),
                SPECIAL_ALERT_COOLDOWN_MINUTES,
                now,
            )

        sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
