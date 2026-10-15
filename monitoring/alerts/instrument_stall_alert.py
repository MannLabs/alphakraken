"""Instrument stall alert checker."""

import logging
from datetime import datetime, timedelta

import pytz

from shared.db.models import KrakenStatus, RawFile

from .base_alert import BaseAlert, Issue
from .config import (
    INSTRUMENT_STALL_INSTRUMENT_IDS,
    INSTRUMENT_STALL_THRESHOLD_HOURS,
    Cases,
)


class InstrumentStallAlert(BaseAlert):
    """Check if no new file has been acquired for a configured time period."""

    def __init__(self):
        """Initialize with empty alerted file names tracker."""
        self._alerted_file_names: dict[str, str] = {}

    @property
    def name(self) -> str:
        """Return the case name for this alert type."""
        return Cases.INSTRUMENT_STALL

    def _get_issues(self, status_objects: list[KrakenStatus]) -> list[Issue[datetime]]:
        """Check for instruments with no recent file acquisition."""
        del status_objects

        if not INSTRUMENT_STALL_INSTRUMENT_IDS:
            return []

        now = datetime.now(pytz.UTC)
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

            created_at = latest.created_at
            if created_at.tzinfo is None:
                created_at = pytz.utc.localize(created_at)

            if created_at < threshold:
                if self._alerted_file_names.get(instrument_id) == latest.original_name:
                    continue
                issues.append(
                    Issue(
                        instrument_id,
                        created_at,
                        relevant_file=latest.original_name,
                        instrument_id=instrument_id,
                    )
                )
                self._alerted_file_names[instrument_id] = latest.original_name
            else:
                self._alerted_file_names.pop(instrument_id, None)

        return issues

    def format_message(self, issues: list[Issue[datetime]]) -> str:
        """Format no new file acquired message."""
        now = datetime.now(pytz.UTC)
        lines = []
        for issue in issues:
            created_at = issue.details
            hours_ago = (now - created_at).total_seconds() / 3600
            lines.append(
                f'- `{issue.identifier}`: last file "{issue.relevant_file}" '
                f"acquired at {created_at.strftime('%Y-%m-%d %H:%M:%S')} UTC "
                f"({hours_ago:.1f} hours ago)"
            )
        instruments_str = "\n".join(lines)
        return f"No new file acquired:\n{instruments_str}"
