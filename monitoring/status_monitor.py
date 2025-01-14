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
from shared.db.models import (
    TERMINAL_STATUSES,
    KrakenStatus,
    KrakenStatusValues,
    RawFile,
)
from shared.keys import EnvVars

# TODO: add unit tests
# TODO: report when error has resolved
# TODO: add a "all is well" message once a day/week?

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

STATUS_PILE_UP_THRESHOLD = 5


class Cases:
    """Cases for which to send alerts."""

    STALE = "stale"
    LOW_DISK_SPACE = "low_disk_space"
    HEALTH_CHECK_FAILED = "health_check_failed"
    STATUS_PILE_UP = "status_pile_up"


def _default_value() -> datetime:
    """Default value for defaultdict to have an alert on the first occurrence."""
    return datetime.now(pytz.UTC) - timedelta(minutes=ALERT_COOLDOWN_MINUTES + 1)


# Track when we last alerted about each issue_type to implement cooldown
last_alerts = defaultdict(_default_value)


def _send_kraken_instrument_alert(
    instruments_with_data: list[tuple[str, datetime | int | str]], case: str
) -> None:
    """Send alert to Slack about stale status."""
    instruments = [instrument_id for instrument_id, _ in instruments_with_data]

    if not _should_send_alert(instruments, case):
        return

    if case == Cases.STALE:
        instruments_str = "\n".join(
            [
                f"- `{instrument_id}`: {updated_at.strftime('%Y-%m-%d %H:%M:%S')} UTC ({(datetime.now(pytz.UTC) - updated_at).total_seconds() / 60 / 60:.1f} hours ago)"
                for instrument_id, updated_at in instruments_with_data
            ]
        )
        message = f"Health check status is stale:\n{instruments_str}."

    elif case == Cases.LOW_DISK_SPACE:
        instruments_str = "\n".join(
            [
                f"- `{instrument_id}`: {disk_space} GB"
                for instrument_id, disk_space in instruments_with_data
            ]
        )
        message = f"Low disk space detected:\n{instruments_str}"

    elif case == Cases.HEALTH_CHECK_FAILED:
        instruments_str = "\n".join(
            [
                f"- `{instrument_id}`: {status_details}"
                for instrument_id, status_details in instruments_with_data
            ]
        )
        message = f"Health check failed:\n{instruments_str}"

    elif case == Cases.STATUS_PILE_UP:
        instruments_str = "\n".join(
            [
                f"- `{instrument_id}`: {status_info}"
                for instrument_id, status_info in instruments_with_data
            ]
        )
        message = f"Status pile up detected:\n{instruments_str}"

    else:
        raise ValueError(f"Unknown case: {case}")

    try:
        _send_slack_message(message)
    except RequestException:
        logging.exception("Failed to send Slack alert.")
    else:
        for instrument_id in instruments:
            last_alerts[f"{case}{instrument_id}"] = datetime.now(pytz.UTC)


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
    low_disk_space_instruments = []
    health_check_failed_instruments = []
    status_pile_up_instruments = []

    kraken_statuses = KrakenStatus.objects
    for kraken_status in kraken_statuses:
        last_updated_at = pytz.utc.localize(kraken_status.updated_at_)
        instrument_id = kraken_status.instrument_id
        if last_updated_at < stale_threshold:
            logging.warning(
                f"Stale status detected for {instrument_id}, "
                f"last update: {last_updated_at}"
            )
            stale_instruments.append((instrument_id, last_updated_at))

        if (free_space_gb := kraken_status.free_space_gb) < FREE_SPACE_THRESHOLD_GB:
            logging.warning(
                f"Low disk space detected for {instrument_id}, "
                f"free space: {free_space_gb} GB"
            )
            low_disk_space_instruments.append((instrument_id, free_space_gb))

        if kraken_status.status != KrakenStatusValues.OK:
            logging.warning(
                f"Error detected for {instrument_id}, "
                f"status details: {kraken_status.status_details}"
            )
            health_check_failed_instruments.append(
                (instrument_id, kraken_status.status_details)
            )

        _append_status_pile_up_instruments(instrument_id, status_pile_up_instruments)

    if stale_instruments:
        _send_kraken_instrument_alert(stale_instruments, Cases.STALE)

    if low_disk_space_instruments:
        _send_kraken_instrument_alert(low_disk_space_instruments, Cases.LOW_DISK_SPACE)

    if health_check_failed_instruments:
        _send_kraken_instrument_alert(
            health_check_failed_instruments, Cases.HEALTH_CHECK_FAILED
        )
    if status_pile_up_instruments:
        _send_kraken_instrument_alert(status_pile_up_instruments, Cases.STATUS_PILE_UP)


def _append_status_pile_up_instruments(
    instrument_id: str, status_pile_up_instruments: list[tuple[str, str]]
) -> None:
    """Add instruments with too many files in non-terminal statuses to status_pile_up_instruments."""
    status_counts = RawFile.objects(
        instrument_id=instrument_id, status__nin=TERMINAL_STATUSES
    ).item_frequencies("status")

    if piled_up_statuses := [
        status
        for status, count in status_counts.items()
        if count > STATUS_PILE_UP_THRESHOLD
    ]:
        piled_up_statuses_str = "; ".join(
            [f"{status}: {status_counts[status]}" for status in piled_up_statuses]
        )
        logging.warning(
            f"Pile up detected for {instrument_id}, {piled_up_statuses_str}"
        )

        status_pile_up_instruments.append((instrument_id, piled_up_statuses_str))


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
