#!/usr/bin/env python3

"""Script to monitor KrakenStatus and send alerts to Slack when health checks get stale."""

import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from time import sleep

import pytz
import requests
from requests.exceptions import RequestException

from shared.db.engine import connect_db
from shared.db.models import KrakenStatus

SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL")
if not SLACK_WEBHOOK_URL:
    logging.error("SLACK_WEBHOOK_URL environment variable must be set")
    sys.exit(1)

# Constants
CHECK_INTERVAL_SECONDS = 60  # How often to check MongoDB
STALE_THRESHOLD_MINUTES = 15  # How old a status can be before considered stale
ALERT_COOLDOWN_MINUTES = (
    120  # Minimum time between repeated alerts for the same instrument
)

# Track when we last alerted about each instrument to implement cooldown
last_alerts = defaultdict(datetime.now)


def send_slack_alert(stale_instruments: list[tuple[str, datetime]]) -> None:
    """Send alert to Slack about stale status."""
    if not _should_send_alert(stale_instruments):
        return

    instruments = ", ".join([instrument_id for instrument_id, _ in stale_instruments])
    oldest_updated_at = min([updated_at for _, updated_at in stale_instruments])

    message = {
        "text": f"🚨 *Alert*: Health check status for `{instruments}` is stale\n"
        f"Last update: {oldest_updated_at.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"
        f"Time since last update: {(datetime.now(pytz.UTC) - pytz.utc.localize(oldest_updated_at)).total_seconds()/60} minutes."
    }

    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=message, timeout=10)
        response.raise_for_status()
        logging.info("Successfully sent Slack alert.")

        for instrument_id, _ in stale_instruments:
            last_alerts[instrument_id] = datetime.now(pytz.UTC)

    except RequestException:
        logging.exception("Failed to send Slack alert.")


def _should_send_alert(stale_instruments: list[tuple[str, datetime]]) -> bool:
    """Check if we should send an alert based on cooldown period."""
    send_alert = False
    for instrument_id, _ in stale_instruments:
        cooldown_time = last_alerts[instrument_id] + timedelta(
            minutes=ALERT_COOLDOWN_MINUTES
        )
        send_alert |= datetime.now(pytz.UTC) > cooldown_time

    return send_alert


def check_kraken_status() -> None:
    """Check KrakenStatus collection for stale entries."""
    now = datetime.now(pytz.UTC)
    stale_threshold = now - timedelta(minutes=STALE_THRESHOLD_MINUTES)

    logging.info("Checking status...")

    stale_instruments = []
    statuses = KrakenStatus.objects
    for status in statuses:
        last_updated_at = pytz.utc.localize(status.updated_at_)
        if last_updated_at < stale_threshold:
            logging.warning(
                f"Stale status detected for {status.instrument_id}, "
                f"last update: {last_updated_at}"
            )
            stale_instruments.append((status.instrument_id, last_updated_at))

    if stale_instruments:
        send_slack_alert(stale_instruments)


def main() -> None:
    """Main monitoring loop."""
    logging.info(
        f"Starting KrakenStatus monitor (check interval: {CHECK_INTERVAL_SECONDS}s, "
        f"stale threshold: {STALE_THRESHOLD_MINUTES}m)"
    )

    while True:
        connect_db()
        try:
            check_kraken_status()
        except Exception:
            logging.exception("Error checking KrakenStatus")

        sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
