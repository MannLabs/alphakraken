"""Unit tests for S3UploadFailureAlert class."""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytz

from monitoring.alerts.base_alert import Issue
from monitoring.alerts.s3_upload_failure_alert import S3UploadFailureAlert
from shared.db.raw_files import RawFileFilter
from shared.domain.raw_files import BackupStatus
from shared.tests.factories import raw_file

NOW = datetime(2024, 1, 2, 12, 0, 0, tzinfo=pytz.UTC)


class TestS3UploadFailureAlert:
    """Test suite for S3UploadFailureAlert class."""

    def test_name_should_return_class_name(self) -> None:
        """Test that name property returns the class name."""
        assert S3UploadFailureAlert().name == "S3UploadFailureAlert"

    def test_is_reported_once_per_file(self) -> None:
        """Each file is reported once, without cooldown."""
        assert S3UploadFailureAlert().cooldown_minutes is None

    def test_format_message_should_format_correctly(self) -> None:
        """Test that format_message produces correct output."""
        # given
        alert = S3UploadFailureAlert()
        issues = [
            Issue("file1", "S3 upload failed | project: proj1 | instrument: inst1"),
            Issue("file2", "S3 upload failed | project: proj2 | instrument: inst2"),
        ]

        # when
        result = alert.format_message(issues)

        # then
        assert result == (
            "- `file1`: S3 upload failed | project: proj1 | instrument: inst1\n"
            "- `file2`: S3 upload failed | project: proj2 | instrument: inst2"
        )

    @patch("shared.db.raw_files.find")
    @patch(
        "monitoring.alerts.s3_upload_failure_alert.RAW_FILE_STATUS_LOOKBACK_HOURS", 24
    )
    def test_get_issues_should_query_recent_failed_files(self, mock_find: Mock) -> None:
        """Only files in UPLOAD_FAILED status updated within the lookback are queried."""
        # given
        mock_find.return_value = []

        # when
        result = S3UploadFailureAlert()._get_issues([], NOW)

        # then
        assert result == []
        mock_find.assert_called_once_with(
            RawFileFilter(
                backup_status=BackupStatus.UPLOAD_FAILED,
                updated_after=NOW - timedelta(hours=24),
            )
        )

    @patch("shared.db.raw_files.find")
    def test_get_issues_should_return_one_issue_per_failed_file(
        self, mock_find: Mock
    ) -> None:
        """Each failed file yields an issue keyed by file id."""
        # given
        mock_find.return_value = [
            raw_file(id="file1", project_id="P1", instrument_id="inst1"),
            raw_file(id="file2", project_id=None, instrument_id=None),
        ]
        alert = S3UploadFailureAlert()

        # when
        first = alert._get_issues([], NOW)
        second = alert._get_issues([], NOW)

        # then
        assert first == [
            Issue(
                "file1",
                "S3 upload failed | project: P1 | instrument: inst1",
                relevant_file="file1",
                instrument_id="inst1",
            ),
            Issue(
                "file2",
                "S3 upload failed | project: N/A | instrument: N/A",
                relevant_file="file2",
                instrument_id=None,
            ),
        ]
        assert [i.key for i in first] == ["file1", "file2"]
        assert second == first  # stateless: the alert manager reports each key once
