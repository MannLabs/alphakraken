#!/usr/bin/env python3

"""Script to monitor AlphaKraken and send alerts to Slack or MS Teams."""

import logging
from time import sleep

from alert_decider import AlertManager, send_db_alert
from config import (
    CHECK_INTERVAL_SECONDS,
    STALE_STATUS_THRESHOLD_MINUTES,
)
from pymongo.errors import ServerSelectionTimeoutError

from shared.db.engine import connect_db

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

    manager = AlertManager()
    while True:
        try:
            connect_db(raise_on_error=True)
        except Exception:  # noqa: BLE001
            send_db_alert("db_connection", manager)

        try:
            manager.check_all()
        except ServerSelectionTimeoutError:
            send_db_alert("db_timeout", manager)

        except Exception:
            logging.exception("Error checking KrakenStatus")

        sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
