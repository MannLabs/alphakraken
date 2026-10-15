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
from datetime import timedelta
from time import sleep
from uuid import uuid4

from active_instance import ActiveInstanceElection
from alert_manager import AlertManager, send_special_alert
from alerts.config import (
    ACTIVE_INSTANCE_TIMEOUT_SECONDS,
    CHECK_INTERVAL_SECONDS,
    STALE_STATUS_THRESHOLD_MINUTES,
)
from messenger_clients import AlertTypes
from pymongo.errors import ServerSelectionTimeoutError
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
    active_instance_election = ActiveInstanceElection(
        instance, timedelta(seconds=ACTIVE_INSTANCE_TIMEOUT_SECONDS)
    )
    # sent by every instance, to see which ones are running
    send_special_alert(
        "general",
        "startup",
        "Monitoring started. ",
        alert_manager,
        AlertTypes.INFO,
    )

    while True:
        try:
            connect_db(raise_on_error=True)
            is_active = active_instance_election.try_become_active()
        except Exception as e:  # noqa: BLE001
            # sent by every instance, as the active one cannot be determined without DB
            send_special_alert(
                "db",
                "db_connection_error",
                f"Error connecting to MongoDB: {e}",
                alert_manager,
            )
            # checking would fail as well and send a second alert
            sleep(CHECK_INTERVAL_SECONDS)
            continue

        try:
            alert_manager.check_for_issues(is_active=is_active)
        except ServerSelectionTimeoutError:
            send_special_alert(
                "db", "db_timeout", "Error connecting to MongoDB", alert_manager
            )

        except Exception as e:
            logging.exception("Error checking KrakenStatus")
            if is_active:
                error_line = extract_error_line(e)
                send_special_alert(
                    "general",
                    "exception",
                    f"{type(e).__name__} during checking alerts: {e} at: {error_line}",
                    alert_manager,
                )

        sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
