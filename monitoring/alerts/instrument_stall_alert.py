"""Instrument stall alert checker."""

import logging
from datetime import datetime, timedelta

from utils import ensure_utc

from shared.db.models import KrakenStatus, RawFile

from .base_alert import BaseAlert, Issue
from .config import (
    INSTRUMENT_STALL_INSTRUMENT_IDS,
    INSTRUMENT_STALL_THRESHOLD_HOURS,
)


class InstrumentStallAlert(BaseAlert):
    """Check if no new file has been acquired for a configured time period."""

    title = "No new file acquired"

    def __init__(self):
        """Initialize with empty alerted file names tracker."""
        self._alerted_file_names: dict[str, str] = {}

    def _get_issues(
        self, status_objects: list[KrakenStatus], now: datetime
    ) -> list[Issue[str]]:
        """Check for instruments with no recent file acquisition."""
        del status_objects

        if not INSTRUMENT_STALL_INSTRUMENT_IDS:
            return []

        threshold = now - timedelta(hours=INSTRUMENT_STALL_THRESHOLD_HOURS)

        issues = []
        for instrument_id in INSTRUMENT_STALL_INSTRUMENT_IDS:
            latest = (
                RawFile.objects.filter(instrument_id=instrument_id)
                .order_by("-created_at")
                .only("original_name", "created_at")
                .first()
            )

            if not latest:
                logging.debug(f"No raw files found for instrument {instrument_id}")
                continue

            created_at = ensure_utc(latest.created_at)

            if created_at < threshold:
                if self._alerted_file_names.get(instrument_id) == latest.original_name:
                    continue
                hours_ago = (now - created_at).total_seconds() / 3600
                issues.append(
                    Issue(
                        instrument_id,
                        f'last file "{latest.original_name}" acquired at '
                        f"{created_at:%Y-%m-%d %H:%M:%S} UTC ({hours_ago:.1f} hours ago)",
                        relevant_file=latest.original_name,
                        instrument_id=instrument_id,
                    )
                )
                self._alerted_file_names[instrument_id] = latest.original_name
            else:
                self._alerted_file_names.pop(instrument_id, None)

        return issues
