"""S3 upload failure alert checker."""

from datetime import datetime, timedelta

import pytz

from shared.db.models import BackupStatus, KrakenStatus, RawFile

from .base_alert import BaseAlert, Issue
from .config import CHECK_INTERVAL_SECONDS


class S3UploadFailureAlert(BaseAlert):
    """Check for raw files that have transitioned to UPLOAD_FAILED status."""

    def __init__(self):
        """Initialize with previous upload statuses."""
        self.previous_upload_statuses = {}

    def _get_issues(self, status_objects: list[KrakenStatus]) -> list[Issue[str]]:
        """Check for raw files that have changed to UPLOAD_FAILED status."""
        del status_objects

        youngest_updated_at = datetime.now(pytz.UTC) - timedelta(
            seconds=CHECK_INTERVAL_SECONDS * 5
        )
        recently_updated_raw_files = RawFile.objects.filter(
            updated_at___gt=youngest_updated_at
        ).only(
            "id",
            "backup_status",
            "project_id",
            "instrument_id",
        )

        new_failure_files = []
        current_statuses = {}

        for raw_file in recently_updated_raw_files:
            raw_file_id = raw_file.id
            current_status = raw_file.backup_status
            current_statuses[raw_file_id] = current_status

            if (
                current_status == BackupStatus.UPLOAD_FAILED
                and current_status != self.previous_upload_statuses.get(raw_file_id)
            ):
                failure_info = self._build_failure_info(raw_file)
                new_failure_files.append(
                    Issue(
                        raw_file_id,
                        failure_info,
                        relevant_file=raw_file_id,
                        instrument_id=raw_file.instrument_id,
                    )
                )

        self.previous_upload_statuses.update(current_statuses)

        return new_failure_files

    def _build_failure_info(self, raw_file: RawFile) -> str:
        """Build a descriptive string for the S3 upload failure."""
        project = raw_file.project_id or "N/A"
        instrument = raw_file.instrument_id or "N/A"

        return f"S3 upload failed | project: {project} | instrument: {instrument}"

    def format_message(self, issues: list[Issue[str]]) -> str:
        """Format S3 upload failure message."""
        files_str = "\n".join(
            [f"- `{issue.identifier}`: {issue.details}" for issue in issues]
        )
        return f"S3 upload failures detected:\n{files_str}"
