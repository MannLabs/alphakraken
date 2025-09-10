"""Decide when to send alerts based on monitoring checks."""
import logging
from collections import defaultdict
from datetime import datetime, timedelta

import pytz
from config import (
    ALERT_COOLDOWN_MINUTES,
    BACKUP_FREE_SPACE_THRESHOLD_GB,
    CHECK_INTERVAL_SECONDS,
    FILE_REMOVER_STALE_THRESHOLD_HOURS,
    FREE_SPACE_THRESHOLD_GB,
    INSTRUMENT_FILE_MIN_AGE_HOURS,
    INSTRUMENT_FILE_PILE_UP_THRESHOLDS,
    OUTPUT_FREE_SPACE_THRESHOLD_GB,
    STALE_STATUS_THRESHOLD_MINUTES,
    STATUS_PILE_UP_THRESHOLDS,
    Cases,
)
from messenger_clients import send_message
from requests.exceptions import RequestException

from shared.db.interface import get_raw_files_by_instrument_file_status
from shared.db.models import (
    TERMINAL_STATUSES,
    KrakenStatus,
    KrakenStatusEntities,
    KrakenStatusValues,
    RawFile,
    RawFileStatus,
)


def _default_value() -> datetime:
    """Default value for defaultdict to have an alert on the first occurrence."""
    return datetime.now(pytz.UTC) - timedelta(minutes=ALERT_COOLDOWN_MINUTES + 1)


# Track when we last alerted about each issue_type to implement cooldown
last_alerts = defaultdict(_default_value)

# Track previous raw file statuses to detect changes to ERROR
previous_raw_file_statuses = {}


def _send_kraken_instrument_alert(  # noqa: C901 # too complex
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

    elif case == Cases.INSTRUMENT_FILE_PILE_UP:
        instruments_str = "\n".join(
            [
                f"- `{instrument_id}`: {file_info}"
                for instrument_id, file_info in instruments_with_data
            ]
        )
        message = f"Instrument file pile up detected (files not moved/purged):\n{instruments_str}"

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
        send_message(message)
    except RequestException:
        logging.exception("Failed to send message.")
    else:
        for instrument_id in instruments:
            last_alerts[f"{case}{instrument_id}"] = datetime.now(pytz.UTC)


def _should_send_alert(issue_types: list[str], case: str) -> bool:
    """Check if we should send an alert based on cooldown period."""
    send_alert = False
    for issue_type in issue_types:
        cooldown_time = last_alerts[f"{case}{issue_type}"] + timedelta(
            minutes=ALERT_COOLDOWN_MINUTES
        )
        send_alert |= datetime.now(pytz.UTC) > cooldown_time

    return send_alert


def check_kraken_update_status() -> None:  # noqa: PLR0912, PLR0915, C901 Too many branches, too many statement, too complex
    """Check KrakenStatus collection for stale entries."""
    now = datetime.now(pytz.UTC)
    stale_threshold = now - timedelta(minutes=STALE_STATUS_THRESHOLD_MINUTES)
    file_remover_stale_threshold = now - timedelta(
        hours=FILE_REMOVER_STALE_THRESHOLD_HOURS
    )

    logging.info("Checking kraken update status...")

    stale_instruments = []
    low_disk_space_instruments = []
    health_check_failed_instruments = []
    status_pile_up_instruments = []
    instrument_file_pile_up_instruments = []

    kraken_statuses = KrakenStatus.objects
    for kraken_status in kraken_statuses:
        last_updated_at = pytz.utc.localize(kraken_status.updated_at_)
        id_ = kraken_status.id

        # Use different stale thresholds based on entry type
        if (
            kraken_status.entity_type == KrakenStatusEntities.JOB
            and id_ == "file_remover"
        ):
            threshold_to_use = file_remover_stale_threshold
        else:
            threshold_to_use = stale_threshold

        if last_updated_at < threshold_to_use:
            logging.warning(
                f"Stale status detected for {id_}, last update: {last_updated_at}"
            )
            stale_instruments.append((id_, last_updated_at))

        # Check disk space only for file systems and instruments (not for job-type entries)
        if kraken_status.entity_type != KrakenStatusEntities.JOB:
            # Determine threshold based on entry type
            if kraken_status.entity_type == KrakenStatusEntities.FILE_SYSTEM:
                if id_ == "backup":
                    threshold = BACKUP_FREE_SPACE_THRESHOLD_GB
                elif id_ == "output":
                    threshold = OUTPUT_FREE_SPACE_THRESHOLD_GB
                else:
                    threshold = (
                        FREE_SPACE_THRESHOLD_GB  # Default for unknown filesystem
                    )
            else:
                threshold = FREE_SPACE_THRESHOLD_GB  # For instruments

            if (free_space_gb := kraken_status.free_space_gb) < threshold:
                logging.warning(
                    f"Low disk space detected for {id_} ({kraken_status.entity_type}), "
                    f"free space: {free_space_gb} GB, threshold: {threshold} GB"
                )
                low_disk_space_instruments.append((id_, free_space_gb))

        if kraken_status.status != KrakenStatusValues.OK:
            logging.warning(
                f"Error detected for {id_}, "
                f"status details: {kraken_status.status_details}"
            )
            health_check_failed_instruments.append((id_, kraken_status.status_details))

        # Only check for status pile-up on instruments (not job or file_system types)
        if kraken_status.entity_type == KrakenStatusEntities.INSTRUMENT:
            _append_status_pile_up_instruments(id_, status_pile_up_instruments)
            _append_instrument_file_pile_up_instruments(
                id_, instrument_file_pile_up_instruments
            )

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

    if instrument_file_pile_up_instruments:
        _send_kraken_instrument_alert(
            instrument_file_pile_up_instruments, Cases.INSTRUMENT_FILE_PILE_UP
        )

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


def _append_instrument_file_pile_up_instruments(
    instrument_id: str, instrument_file_pile_up_instruments: list[tuple[str, str]]
) -> None:
    """Add instruments with too many files stuck in instrument file statuses to instrument_file_pile_up_instruments."""
    pile_up_detected = []

    for instrument_file_status, threshold in INSTRUMENT_FILE_PILE_UP_THRESHOLDS.items():
        files = get_raw_files_by_instrument_file_status(
            instrument_file_status,
            instrument_id=instrument_id,
            min_age_hours=INSTRUMENT_FILE_MIN_AGE_HOURS,
        )
        count = len(files)

        if count > threshold:
            pile_up_detected.append(f"{instrument_file_status}: {count}")
            logging.warning(
                f"Instrument file pile up detected for {instrument_id}, {instrument_file_status}: {count} files (threshold: {threshold})"
            )

    if pile_up_detected:
        pile_up_str = "; ".join(pile_up_detected)
        instrument_file_pile_up_instruments.append((instrument_id, pile_up_str))


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


def send_db_alert(error_type: str) -> None:
    """Send message about MongoDB connection error."""
    if not _should_send_alert([error_type], "db"):
        return

    logging.info("Error connecting to MongoDB")

    message = f"Error connecting to MongoDB: {error_type}"
    send_message(message)

    last_alerts[error_type] = datetime.now(pytz.UTC)
