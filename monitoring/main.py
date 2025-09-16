#!/usr/bin/env python3

"""Script to monitor AlphaKraken and send alerts to Slack or MS Teams."""

import logging
from time import sleep

from alert_manager import AlertManager, send_special_alert
from alerts.config import (
    CHECK_INTERVAL_SECONDS,
    STALE_STATUS_THRESHOLD_MINUTES,
)
from pymongo.errors import ServerSelectionTimeoutError

from shared.db.engine import connect_db

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# TODO: add unit tests
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
    while True:
        try:
            connect_db(raise_on_error=True)
        except Exception as e:  # noqa: BLE001
            send_special_alert(
                "db",
                "db_connection_error",
                f"Error connecting to MongoDB: {e}",
                alert_manager,
            )

        try:
            alert_manager.check_for_issues()
        except ServerSelectionTimeoutError:
            send_special_alert(
                "db", "db_timeout", "Error connecting to MongoDB", alert_manager
            )

        except Exception as e:
            logging.exception("Error checking KrakenStatus")
            send_special_alert(
                "general",
                "exception",
                f"Exception during checking alerts: {e}",
                alert_manager,
            )

        sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
