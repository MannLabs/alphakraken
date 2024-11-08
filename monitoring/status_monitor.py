#!/usr/bin/env python3

"""Script to monitor KrakenStatus and send alerts to Slack when health checks get stale."""

import logging
import os
import sys
from datetime import datetime, timedelta
from time import sleep

import pytz
import requests
from requests.exceptions import RequestException

from shared.db.engine import connect_db
from shared.db.models import KrakenStatus

SLACK_WEBHOOK_URL = os.environ.get(
    "SLACK_WEBHOOK_URL",
    "https://hooks.slack.com/services/...",
)
if not SLACK_WEBHOOK_URL:
    logging.error("SLACK_WEBHOOK_URL environment variable must be set")
    sys.exit(1)

# Constants
CHECK_INTERVAL_SECONDS = 5  # How often to check MongoDB
STALE_THRESHOLD_MINUTES = 15  # How old a status can be before considered stale
ALERT_COOLDOWN_MINUTES = (
    120  # Minimum time between repeated alerts for the same instrument
)

# Track when we last alerted about each instrument to implement cooldown
last_alerts = {}


def send_slack_alert(instrument_id: str, last_update: datetime) -> None:
    """Send alert to Slack about stale status."""
    if not _should_send_alert(instrument_id):
        return

    message = {
        "text": f"🚨 *Alert*: Health check status for `{instrument_id}` is stale\n"
        f"Last update: {last_update.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
        f"Time since last update: {(datetime.now(pytz.UTC) - pytz.utc.localize(last_update)).total_seconds()/60} minutes."
    }

    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=message, timeout=10)
        response.raise_for_status()
        logging.info(f"Successfully sent Slack alert for {instrument_id}")

        logging.error("2")
        last_alerts[instrument_id] = datetime.now(pytz.UTC)
    except RequestException:
        logging.exception("Failed to send Slack alert.")


def _should_send_alert(instrument_id: str) -> bool:
    """Check if we should send an alert based on cooldown period."""
    if instrument_id not in last_alerts:
        return True

    cooldown_time = last_alerts[instrument_id] + timedelta(
        minutes=ALERT_COOLDOWN_MINUTES
    )

    return datetime.now(pytz.UTC) > cooldown_time


def check_kraken_status() -> None:
    """Check KrakenStatus collection for stale entries."""
    now = datetime.now(pytz.UTC)
    stale_threshold = now - timedelta(minutes=STALE_THRESHOLD_MINUTES)

    try:
        statuses = KrakenStatus.objects
        for status in statuses:
            if pytz.utc.localize(status.updated_at_) < stale_threshold:
                logging.warning(
                    f"Stale status detected for {status.instrument_id}, "
                    f"last update: {status.updated_at_}"
                )
                send_slack_alert(status.instrument_id, status.updated_at_)
            else:
                logging.debug(
                    f"Status for {status.instrument_id} is current, "
                    f"last update: {status.updated_at_}"
                )
    except Exception:
        logging.exception("Error checking KrakenStatus")


def main() -> None:
    """Main monitoring loop."""
    connect_db()

    logging.info(
        f"Starting KrakenStatus monitor (check interval: {CHECK_INTERVAL_SECONDS}s, "
        f"stale threshold: {STALE_THRESHOLD_MINUTES}m)"
    )

    while True:
        check_kraken_status()
        sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
