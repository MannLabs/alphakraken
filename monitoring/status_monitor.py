#!/usr/bin/env python3

"""Script to monitor AlphaKraken and send alerts to Slack or MS Teams."""

import json
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
    RawFileStatus,
)
from shared.keys import EnvVars

# TODO: add unit tests
# TODO: report when error has resolved
# TODO: add a "all is well" message once a day/week?
# TODO: health check if webapp is reachable

MESSENGER_WEBHOOK_URL: str = os.environ.get(EnvVars.MESSENGER_WEBHOOK_URL, "")
if not MESSENGER_WEBHOOK_URL:
    logging.error(f"{EnvVars.MESSENGER_WEBHOOK_URL} environment variable must be set")
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

STATUS_PILE_UP_THRESHOLDS = defaultdict(lambda: 5)
STATUS_PILE_UP_THRESHOLDS["quanting"] = 10


class Cases:
    """Cases for which to send alerts."""

    STALE = "stale"
    LOW_DISK_SPACE = "low_disk_space"
    HEALTH_CHECK_FAILED = "health_check_failed"
    STATUS_PILE_UP = "status_pile_up"
    RAW_FILE_ERROR = "raw_file_error"


def _default_value() -> datetime:
    """Default value for defaultdict to have an alert on the first occurrence."""
    return datetime.now(pytz.UTC) - timedelta(minutes=ALERT_COOLDOWN_MINUTES + 1)


# Track when we last alerted about each issue_type to implement cooldown
last_alerts = defaultdict(_default_value)

# Track previous raw file statuses to detect changes to ERROR
previous_raw_file_statuses = {}


def _send_kraken_instrument_alert(
    instruments_with_data: list[tuple[str, datetime | int | str]], case: str
) -> None:
    """Send alert about stale status."""
    instruments = [instrument_id for instrument_id, _ in instruments_with_data]

    if not _should_send_alert(instruments, case):
        return

    if case == Cases.STALE:
        instruments_str = "\n".join(
            [
                f"- `{instrument_id}`: {updated_at.strftime('%Y-%m-%d %H:%M:%S')} UTC ({(datetime.now(pytz.UTC) - updated_at).total_seconds() / 60 / 60:.1f} hours ago)"  # ty: ignore[possibly-unbound-attribute]
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

    elif case == Cases.RAW_FILE_ERROR:
        files_str = "\n".join(
            [
                f"- `{file_id}`: {status_details}"
                for file_id, status_details in instruments_with_data
            ]
        )
        message = f"Raw files changed to ERROR status:\n{files_str}"

    else:
        raise ValueError(f"Unknown case: {case}")

    try:
        _send_message(message)
    except RequestException:
        logging.exception("Failed to send message.")
    else:
        for instrument_id in instruments:
            last_alerts[f"{case}{instrument_id}"] = datetime.now(pytz.UTC)


def _send_message(message: str) -> None:
    """Send message to Slack or MS Teams."""
    # TODO: this could be more elegant
    if MESSENGER_WEBHOOK_URL.startswith("https://hooks.slack.com"):
        _send_slack_message(message)
    else:
        _send_msteams_message(message)


def _send_slack_message(message: str) -> None:
    env_name = os.environ.get(EnvVars.ENV_NAME)

    prefix = "🚨 <!channel> " if env_name == "production" else ""
    payload = {
        "text": f"{prefix} [{env_name}] *Alert*: {message}",
    }
    response = requests.post(MESSENGER_WEBHOOK_URL, json=payload, timeout=10)
    response.raise_for_status()
    logging.info("Successfully sent Slack message.")


def _send_msteams_message(message: str) -> None:
    # Define the adaptive card JSON
    adaptive_card_json = {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "body": [{"type": "TextBlock", "text": message}],
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.0",
                },
            }
        ],
    }

    response = requests.post(
        MESSENGER_WEBHOOK_URL,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        data=json.dumps(adaptive_card_json),
        timeout=10,
    )
    response.raise_for_status()
    logging.info("Successfully sent MS Teams message.")


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

    # Check for raw files that have changed to ERROR status
    new_error_files = _get_raw_files_in_error_status()
    if new_error_files:
        _send_kraken_instrument_alert(new_error_files, Cases.RAW_FILE_ERROR)  # type: ignore[possibly-unbound-attribute]


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
        if count > STATUS_PILE_UP_THRESHOLDS[status]
    ]:
        piled_up_statuses_str = "; ".join(
            [f"{status}: {status_counts[status]}" for status in piled_up_statuses]
        )
        logging.warning(
            f"Pile up detected for {instrument_id}, {piled_up_statuses_str}"
        )

        status_pile_up_instruments.append((instrument_id, piled_up_statuses_str))


def _get_raw_files_in_error_status() -> list[tuple[str, str]]:
    """Get raw files that have changed to ERROR status since last check."""
    global previous_raw_file_statuses  # noqa: PLW0603

    youngest_updated_at = (
        datetime.now(pytz.UTC)
        - timedelta(
            seconds=CHECK_INTERVAL_SECONDS
            * 5  # only considering files updated in the last 5 checks to avoid false alerts on monitor restarts
        )
    )
    recently_updated_raw_files = RawFile.objects.filter(
        updated_at___gt=youngest_updated_at
    ).only("id", "status", "status_details")

    new_error_files = []
    current_statuses = {}

    for raw_file in recently_updated_raw_files:
        raw_file_id = raw_file.id
        current_status = raw_file.status
        current_statuses[raw_file_id] = current_status

        # Check if this file has changed to ERROR status
        if (
            current_status == RawFileStatus.ERROR
            and current_status != previous_raw_file_statuses.get(raw_file_id)
        ):
            status_details = raw_file.status_details or "None"
            logging.warning(
                f"Raw file {raw_file_id} changed to ERROR status. Details: {status_details}"
            )
            new_error_files.append((raw_file_id, status_details))

    # Update the previous statuses for next check
    previous_raw_file_statuses = current_statuses

    return new_error_files


def _send_db_alert(error_type: str) -> None:
    """Send message about MongoDB connection error."""
    if not _should_send_alert([error_type], "db"):
        return

    logging.info("Error connecting to MongoDB")

    message = f"Error connecting to MongoDB: {error_type}"
    _send_message(message)

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
