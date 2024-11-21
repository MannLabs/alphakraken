#!/usr/bin/env python3

"""Script to monitor AlphaKraken and send alerts to Slack."""

import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from time import sleep

import pytz
import requests
from pymongo.errors import ServerSelectionTimeoutError
from requests.exceptions import RequestException

from shared.db.engine import connect_db
from shared.db.models import KrakenStatus
from shared.keys import EnvVars

# TODO: add unit tests
# TODO: only alert in production?
# TODO: report when error has resolved
# TODO: add a "all is well" message once a day/week?

# TODO: add alert on failed health check here
# TODO: add alert on pile up in non-terminal states

SLACK_WEBHOOK_URL = os.environ.get(EnvVars.SLACK_WEBHOOK_URL)
if not SLACK_WEBHOOK_URL:
    logging.error(f"{EnvVars.SLACK_WEBHOOK_URL} environment variable must be set")
    sys.exit(1)

# Constants
CHECK_INTERVAL_SECONDS = 60
ALERT_COOLDOWN_MINUTES = (
    120  # Minimum time between repeated alerts for the same issue_type
)

STALE_STATUS_THRESHOLD_MINUTES = (
    15  # How old a kraken status can be before considered stale
)
FREE_SPACE_THRESHOLD_GB = (
    200  # regardless of the configuration in airflow: 200 GB is very low
)


class Cases:
    """Cases for which to send alerts."""

    STALE = "stale"
    DISK_SPACE = "disk_space"


def _default_value() -> datetime:
    """Default value for defaultdict to have an alert on the first occurrence."""
    return datetime.now(pytz.UTC) - timedelta(minutes=ALERT_COOLDOWN_MINUTES + 1)


# Track when we last alerted about each issue_type to implement cooldown
last_alerts = defaultdict(_default_value)


def _send_kraken_instrument_alert(
    instruments_with_data: list[tuple[str, datetime | str]], case: str
) -> None:
    """Send alert to Slack about stale status."""
    instruments = [instrument_id for instrument_id, _ in instruments_with_data]

    if not _should_send_alert(instruments, case):
        return

    if case == Cases.STALE:
        instruments_str = ", ".join(instruments)
        oldest_updated_at = min([updated_at for _, updated_at in instruments_with_data])

        message = (
            f"Health check status for `{instruments_str}` is stale\n"
            f"Last update: {oldest_updated_at.strftime('%Y-%m-%d %H:%M:%S')} UTC\n"  # pytype: disable=attribute-error
            f"Time since last update: {(datetime.now(pytz.UTC) - oldest_updated_at).total_seconds()/60/60:.1f} hours."
        )
    elif case == Cases.DISK_SPACE:
        instruments_str = ", ".join(
            [
                f"{instrument_id}: {space} GB"
                for instrument_id, space in instruments_with_data
            ]
        )
        message = f"Low disk space detected: {instruments_str}"
    else:
        raise ValueError(f"Unknown case: {case}")

    try:
        _send_slack_message(message)

        for instrument_id in instruments:
            last_alerts[f"{case}{instrument_id}"] = datetime.now(pytz.UTC)

    except RequestException:
        logging.exception("Failed to send Slack alert.")


def _send_slack_message(message: str) -> None:
    env_name = os.environ.get(EnvVars.ENV_NAME)

    prefix = "🚨 <!channel> " if env_name == "production" else ""
    payload = {
        "text": f"{prefix} [{env_name}] *Alert*: {message}",
    }
    response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
    response.raise_for_status()
    logging.info("Successfully sent Slack alert.")


def _should_send_alert(issue_types: list[str], case: str) -> bool:
    """Check if we should send an alert based on cooldown period."""
    send_alert = False
    for issue_type in issue_types:
        cooldown_time = last_alerts[f"{case}{issue_type}"] + timedelta(
            minutes=ALERT_COOLDOWN_MINUTES
        )
        send_alert |= datetime.now(pytz.UTC) > cooldown_time

    return send_alert


def _check_kraken_update_status() -> None:
    """Check KrakenStatus collection for stale entries."""
    now = datetime.now(pytz.UTC)
    stale_threshold = now - timedelta(minutes=STALE_STATUS_THRESHOLD_MINUTES)

    logging.info("Checking kraken update status...")

    stale_instruments = []
    low_disk_instruments = []
    kraken_statuses = KrakenStatus.objects
    for kraken_status in kraken_statuses:
        last_updated_at = pytz.utc.localize(kraken_status.updated_at_)
        if last_updated_at < stale_threshold:
            logging.warning(
                f"Stale status detected for {kraken_status.instrument_id}, "
                f"last update: {last_updated_at}"
            )
            stale_instruments.append((kraken_status.instrument_id, last_updated_at))

        if free_space_gb := kraken_status.free_space_gb < FREE_SPACE_THRESHOLD_GB:
            logging.warning(
                f"Low disk space detected for {kraken_status.instrument_id}, "
                f"free space: {free_space_gb} GB"
            )
            low_disk_instruments.append((kraken_status.instrument_id, free_space_gb))

    if stale_instruments:
        _send_kraken_instrument_alert(stale_instruments, Cases.STALE)

    if low_disk_instruments:
        _send_kraken_instrument_alert(low_disk_instruments, Cases.DISK_SPACE)


def _send_db_alert(error_type: str) -> None:
    """Send alert to Slack about MongoDB connection error."""
    if not _should_send_alert([error_type], "db"):
        return

    logging.info("Error connecting to MongoDB")

    message = f"Error connecting to MongoDB: {error_type}"
    _send_slack_message(message)

    last_alerts[error_type] = datetime.now(pytz.UTC)


def main() -> None:
    """Main monitoring loop."""
    logging.info(
        f"Starting KrakenStatus monitor (check interval: {CHECK_INTERVAL_SECONDS}s, "
        f"stale threshold: {STALE_STATUS_THRESHOLD_MINUTES}m)"
    )

    while True:
        try:
            connect_db(raise_on_error=True)
        except Exception:  # noqa: BLE001
            _send_db_alert("db_connection")

        try:
            _check_kraken_update_status()
        except ServerSelectionTimeoutError:
            _send_db_alert("db_timeout")

        except Exception:
            logging.exception("Error checking KrakenStatus")

        sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
