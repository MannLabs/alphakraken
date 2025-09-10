"""Raw file error alert checker."""

from datetime import datetime, timedelta

import config
import pytz
from config import Cases

from shared.db.models import KrakenStatus, RawFile, RawFileStatus

from .base_alert import BaseAlert


class RawFileErrorAlert(BaseAlert):
    """Check for raw files that have changed to ERROR status."""

    def __init__(self):
        """Initialize with previous statuses."""
        self.previous_raw_file_statuses = {}

    @property
    def name(self) -> str:
        """Return the case name for this alert type."""
        return Cases.RAW_FILE_ERROR

    def get_issues(self, status_objects: list[KrakenStatus]) -> list[tuple[str, str]]:
        """Check for raw files that have changed to ERROR status."""
        del status_objects

        youngest_updated_at = datetime.now(pytz.UTC) - timedelta(
            seconds=config.CHECK_INTERVAL_SECONDS * 5
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
