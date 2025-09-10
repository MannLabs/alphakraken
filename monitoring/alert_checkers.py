"""Alert checker classes for different types of monitoring alerts."""

import logging
from abc import ABC, abstractmethod
from datetime import datetime, timedelta

import pytz
from mongoengine import QuerySet

from shared.db.interface import get_raw_files_by_instrument_file_status
from shared.db.models import (
    TERMINAL_STATUSES,
    KrakenStatusEntities,
    KrakenStatusValues,
    RawFile,
    RawFileStatus,
)


class BaseAlert(ABC):
    """Base class for all alert checkers."""

    def __init__(self, config):
        """Initialize with configuration (thresholds, etc)."""
        self.config = config

    @abstractmethod
    def check(self, kraken_statuses: QuerySet) -> list[tuple]:
        """Check for issues and return list of (identifier, details) tuples.

        Returns empty list if no issues found.
        """

    @abstractmethod
    def format_message(self, issues: list[tuple]) -> str:
        """Format the alert message for the issues found."""

    @property
    @abstractmethod
    def case_name(self) -> str:
        """Return the case name for this alert type."""


class StaleStatusAlert(BaseAlert):
    """Check for stale status updates."""

    @property
    def case_name(self) -> str:
        """Return the case name for this alert type."""
        return self.config.Cases.STALE

    def check(self, kraken_statuses: QuerySet) -> list[tuple[str, datetime]]:
        """Check for stale statuses."""
        now = datetime.now(pytz.UTC)
        stale_threshold = now - timedelta(
            minutes=self.config.STALE_STATUS_THRESHOLD_MINUTES
        )
        file_remover_stale_threshold = now - timedelta(
            hours=self.config.FILE_REMOVER_STALE_THRESHOLD_HOURS
        )

        stale_instruments = []
        for kraken_status in kraken_statuses:
            last_updated_at = pytz.utc.localize(kraken_status.updated_at_)
            id_ = kraken_status.id

            # Use different thresholds based on entry type
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

        return stale_instruments

    def format_message(self, issues: list[tuple[str, datetime]]) -> str:
        """Format stale status message."""
        instruments_str = "\n".join(
            [
                f"- `{instrument_id}`: {updated_at.strftime('%Y-%m-%d %H:%M:%S')} UTC "
                f"({(datetime.now(pytz.UTC) - updated_at).total_seconds() / 60 / 60:.1f} hours ago)"
                for instrument_id, updated_at in issues
            ]
        )
        return f"Health check status is stale:\n{instruments_str}."


class DiskSpaceAlert(BaseAlert):
    """Check for low disk space."""

    @property
    def case_name(self) -> str:
        """Return the case name for this alert type."""
        return self.config.Cases.LOW_DISK_SPACE

    def check(self, kraken_statuses: QuerySet) -> list[tuple[str, int]]:
        """Check for low disk space."""
        low_disk_space_instruments = []

        for kraken_status in kraken_statuses:
            # Skip job-type entries
            if kraken_status.entity_type == KrakenStatusEntities.JOB:
                continue

            # Determine threshold based on entry type
            id_ = kraken_status.id
            if kraken_status.entity_type == KrakenStatusEntities.FILE_SYSTEM:
                if id_ == "backup":
                    threshold = self.config.BACKUP_FREE_SPACE_THRESHOLD_GB
                elif id_ == "output":
                    threshold = self.config.OUTPUT_FREE_SPACE_THRESHOLD_GB
                else:
                    threshold = self.config.FREE_SPACE_THRESHOLD_GB
            else:
                threshold = self.config.FREE_SPACE_THRESHOLD_GB

            if (free_space_gb := kraken_status.free_space_gb) < threshold:
                logging.warning(
                    f"Low disk space detected for {id_} ({kraken_status.entity_type}), "
                    f"free space: {free_space_gb} GB, threshold: {threshold} GB"
                )
                low_disk_space_instruments.append((id_, free_space_gb))

        return low_disk_space_instruments

    def format_message(self, issues: list[tuple[str, int]]) -> str:
        """Format disk space message."""
        instruments_str = "\n".join(
            [
                f"- `{instrument_id}`: {disk_space} GB"
                for instrument_id, disk_space in issues
            ]
        )
        return f"Low disk space detected:\n{instruments_str}"


class HealthCheckAlert(BaseAlert):
    """Check for failed health checks."""

    @property
    def case_name(self) -> str:
        """Return the case name for this alert type."""
        return self.config.Cases.HEALTH_CHECK_FAILED

    def check(self, kraken_statuses: QuerySet) -> list[tuple[str, str]]:
        """Check for health check failures."""
        health_check_failed_instruments = []

        for kraken_status in kraken_statuses:
            if kraken_status.status != KrakenStatusValues.OK:
                id_ = kraken_status.id
                status_details = kraken_status.status_details
                logging.warning(
                    f"Error detected for {id_}, status details: {status_details}"
                )
                health_check_failed_instruments.append((id_, status_details))

        return health_check_failed_instruments

    def format_message(self, issues: list[tuple[str, str]]) -> str:
        """Format health check failure message."""
        instruments_str = "\n".join(
            [
                f"- `{instrument_id}`: {status_details}"
                for instrument_id, status_details in issues
            ]
        )
        return f"Health check failed:\n{instruments_str}"


class StatusPileUpAlert(BaseAlert):
    """Check for status pile-ups (too many files in non-terminal statuses)."""

    @property
    def case_name(self) -> str:
        """Return the case name for this alert type."""
        return self.config.Cases.STATUS_PILE_UP

    def check(self, kraken_statuses: QuerySet) -> list[tuple[str, str]]:
        """Check for status pile-ups on instruments."""
        status_pile_up_instruments = []

        for kraken_status in kraken_statuses:
            # Only check instruments
            if kraken_status.entity_type != KrakenStatusEntities.INSTRUMENT:
                continue

            instrument_id = kraken_status.id
            status_counts = RawFile.objects(
                instrument_id=instrument_id, status__nin=TERMINAL_STATUSES
            ).item_frequencies("status")

            piled_up_statuses = [
                status
                for status, count in status_counts.items()
                if count > self.config.STATUS_PILE_UP_THRESHOLDS[status]
            ]

            if piled_up_statuses:
                piled_up_statuses_str = "; ".join(
                    [
                        f"{status}: {status_counts[status]}"
                        for status in piled_up_statuses
                    ]
                )
                logging.warning(
                    f"Pile up detected for {instrument_id}, {piled_up_statuses_str}"
                )
                status_pile_up_instruments.append(
                    (instrument_id, piled_up_statuses_str)
                )

        return status_pile_up_instruments

    def format_message(self, issues: list[tuple[str, str]]) -> str:
        """Format status pile-up message."""
        instruments_str = "\n".join(
            [
                f"- `{instrument_id}`: {status_info}"
                for instrument_id, status_info in issues
            ]
        )
        return f"Status pile up detected:\n{instruments_str}"


class InstrumentFilePileUpAlert(BaseAlert):
    """Check for instrument file pile-ups (files not moved/purged)."""

    @property
    def case_name(self) -> str:
        """Return the case name for this alert type."""
        return self.config.Cases.INSTRUMENT_FILE_PILE_UP

    def check(self, kraken_statuses: QuerySet) -> list[tuple[str, str]]:
        """Check for instrument file pile-ups."""
        instrument_file_pile_up_instruments = []

        for kraken_status in kraken_statuses:
            # Only check instruments
            if kraken_status.entity_type != KrakenStatusEntities.INSTRUMENT:
                continue

            instrument_id = kraken_status.id
            pile_up_detected = []

            for (
                instrument_file_status,
                threshold,
            ) in self.config.INSTRUMENT_FILE_PILE_UP_THRESHOLDS.items():
                files = get_raw_files_by_instrument_file_status(
                    instrument_file_status,
                    instrument_id=instrument_id,
                    min_age_hours=self.config.INSTRUMENT_FILE_MIN_AGE_HOURS,
                )
                count = len(files)

                if count > threshold:
                    pile_up_detected.append(f"{instrument_file_status}: {count}")
                    logging.warning(
                        f"Instrument file pile up detected for {instrument_id}, "
                        f"{instrument_file_status}: {count} files (threshold: {threshold})"
                    )

            if pile_up_detected:
                pile_up_str = "; ".join(pile_up_detected)
                instrument_file_pile_up_instruments.append((instrument_id, pile_up_str))

        return instrument_file_pile_up_instruments

    def format_message(self, issues: list[tuple[str, str]]) -> str:
        """Format instrument file pile-up message."""
        instruments_str = "\n".join(
            [f"- `{instrument_id}`: {file_info}" for instrument_id, file_info in issues]
        )
        return f"Instrument file pile up detected (files not moved/purged):\n{instruments_str}"


class RawFileErrorAlert(BaseAlert):
    """Check for raw files that have changed to ERROR status."""

    def __init__(self, config):
        """Initialize with configuration and previous statuses."""
        super().__init__(config)
        self.previous_raw_file_statuses = {}

    @property
    def case_name(self) -> str:
        """Return the case name for this alert type."""
        return self.config.Cases.RAW_FILE_ERROR

    def check(self, kraken_statuses: QuerySet) -> list[tuple[str, str]]:
        """Check for raw files that have changed to ERROR status."""
        del kraken_statuses

        youngest_updated_at = datetime.now(pytz.UTC) - timedelta(
            seconds=self.config.CHECK_INTERVAL_SECONDS * 5
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
                and current_status != self.previous_raw_file_statuses.get(raw_file_id)
            ):
                status_details = raw_file.status_details or "None"
                logging.warning(
                    f"Raw file {raw_file_id} changed to ERROR status. Details: {status_details}"
                )
                new_error_files.append((raw_file_id, status_details))

        # Update the previous statuses for next check
        self.previous_raw_file_statuses = current_statuses

        return new_error_files

    def format_message(self, issues: list[tuple[str, str]]) -> str:
        """Format raw file error message."""
        files_str = "\n".join(
            [f"- `{file_id}`: {status_details}" for file_id, status_details in issues]
        )
        return f"Raw files changed to ERROR status:\n{files_str}"
