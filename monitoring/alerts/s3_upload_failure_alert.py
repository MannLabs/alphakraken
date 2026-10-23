"""S3 upload failure alert checker."""

from datetime import datetime, timedelta

from shared.db.models import BackupStatus, KrakenStatus, RawFile

from .base_alert import BaseAlert, Issue
from .config import RAW_FILE_STATUS_LOOKBACK_HOURS


class S3UploadFailureAlert(BaseAlert):
    """Check for raw files in UPLOAD_FAILED backup status."""

    title = "S3 upload failures detected"
    # once per file
    cooldown_minutes = None

    def _get_issues(
        self, status_objects: list[KrakenStatus], now: datetime
    ) -> list[Issue[str]]:
        """Check for raw files in UPLOAD_FAILED backup status that were updated recently."""
        del status_objects

        youngest_updated_at = now - timedelta(hours=RAW_FILE_STATUS_LOOKBACK_HOURS)
        failed_raw_files = RawFile.objects.filter(
            backup_status=BackupStatus.UPLOAD_FAILED,
            updated_at___gt=youngest_updated_at,
        ).only("id", "project_id", "instrument_id")

        return [
            Issue(
                raw_file.id,
                self._build_failure_info(raw_file),
                relevant_file=raw_file.id,
                instrument_id=raw_file.instrument_id,
            )
            for raw_file in failed_raw_files
        ]

    def _build_failure_info(self, raw_file: RawFile) -> str:
        """Build a descriptive string for the S3 upload failure."""
        project = raw_file.project_id or "N/A"
        instrument = raw_file.instrument_id or "N/A"

        return f"S3 upload failed | project: {project} | instrument: {instrument}"
