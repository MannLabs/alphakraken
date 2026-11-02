"""Raw file error alert checker."""

from datetime import datetime, timedelta

from utils import short_digest

from shared.db.models import KrakenStatus, RawFile, RawFileStatus

from .base_alert import BaseAlert, Issue
from .config import RAW_FILE_STATUS_LOOKBACK_HOURS


class RawFileErrorAlert(BaseAlert):
    """Check for raw files in ERROR status."""

    title = "Raw files changed to ERROR status"
    # once per file and error: a file failing again with a different error is reported again
    cooldown_minutes = None

    def _get_issues(
        self, status_objects: list[KrakenStatus], now: datetime
    ) -> list[Issue[str]]:
        """Check for raw files in ERROR status that were updated recently."""
        del status_objects

        youngest_updated_at = now - timedelta(hours=RAW_FILE_STATUS_LOOKBACK_HOURS)
        error_raw_files = RawFile.objects.filter(
            status=RawFileStatus.ERROR, updated_at___gt=youngest_updated_at
        ).only("id", "status_details", "instrument_id")

        issues = []
        for raw_file in error_raw_files:
            status_details = raw_file.status_details or "None"
            issues.append(
                Issue(
                    raw_file.id,
                    status_details,
                    relevant_file=raw_file.id,
                    instrument_id=raw_file.instrument_id,
                    key=f"{raw_file.id}:{short_digest(status_details)}",
                )
            )
        return issues
