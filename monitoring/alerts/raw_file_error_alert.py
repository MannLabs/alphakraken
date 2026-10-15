"""Raw file error alert checker."""

from datetime import datetime, timedelta

import pytz

from shared.db.models import KrakenStatus, RawFile, RawFileStatus

from .base_alert import BaseAlert, Issue
from .config import RAW_FILE_STATUS_LOOKBACK_HOURS, Cases


class RawFileErrorAlert(BaseAlert):
    """Check for raw files in ERROR status."""

    title = "Raw files changed to ERROR status"

    @property
    def name(self) -> str:
        """Return the case name for this alert type."""
        return Cases.RAW_FILE_ERROR

    def _get_issues(self, status_objects: list[KrakenStatus]) -> list[Issue[str]]:
        """Check for raw files in ERROR status that were updated recently."""
        del status_objects

        youngest_updated_at = datetime.now(pytz.UTC) - timedelta(
            hours=RAW_FILE_STATUS_LOOKBACK_HOURS
        )
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
                    # a file failing again with a different error is reported again
                    unique_key=(raw_file.id, status_details),
                )
            )
        return issues
