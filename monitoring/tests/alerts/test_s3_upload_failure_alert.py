"""Unit tests for S3UploadFailureAlert class."""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytz

from monitoring.alerts.base_alert import Issue
from monitoring.alerts.config import Cases
from monitoring.alerts.s3_upload_failure_alert import S3UploadFailureAlert
from shared.db.models import BackupStatus


def _create_mock_raw_file(**kwargs: str | None) -> Mock:
    """Create a mock RawFile with the given attributes."""
    mock_file = Mock()
    for key, value in kwargs.items():
        setattr(mock_file, key, value)
    return mock_file


class TestS3UploadFailureAlert:
    """Test suite for S3UploadFailureAlert class."""

    def test_case_name_should_return_correct_value(self) -> None:
        """Test that name property returns correct value."""
        # given
        alert = S3UploadFailureAlert()

        # when
        result = alert.name

        # then
        assert result == Cases.S3_UPLOAD_FAILURE

    def test_format_message_should_format_correctly(self) -> None:
        """Test that format_message produces correct output."""
        # given
        alert = S3UploadFailureAlert()
        issues = [
            Issue(
                "file1",
                "S3 upload failed | path: s3://bucket/key1 | project: proj1 | instrument: inst1",
            ),
            Issue(
                "file2",
                "S3 upload failed | path: s3://bucket/key2 | project: proj2 | instrument: inst2",
            ),
        ]

        # when
        result = alert.format_message(issues)

        # then
        expected = (
            "S3 upload failures detected:\n"
            "- `file1`: S3 upload failed | path: s3://bucket/key1 | project: proj1 | instrument: inst1\n"
            "- `file2`: S3 upload failed | path: s3://bucket/key2 | project: proj2 | instrument: inst2"
        )
        assert result == expected

    @patch("monitoring.alerts.s3_upload_failure_alert.RawFile")
    @patch("monitoring.alerts.s3_upload_failure_alert.datetime")
    @patch(
        "monitoring.alerts.s3_upload_failure_alert.RAW_FILE_STATUS_LOOKBACK_HOURS", 24
    )
    def test_get_issues_should_query_recent_failed_files(
        self, mock_datetime: Mock, mock_raw_file: Mock
    ) -> None:
        """Only files in UPLOAD_FAILED status updated within the lookback are queried."""
        # given
        now = datetime(2024, 1, 2, 12, 0, 0, tzinfo=pytz.UTC)
        mock_datetime.now.return_value = now
        mock_raw_file.objects.filter.return_value.only.return_value = []

        # when
        result = S3UploadFailureAlert()._get_issues([])

        # then
        assert result == []
        mock_raw_file.objects.filter.assert_called_once_with(
            backup_status=BackupStatus.UPLOAD_FAILED,
            updated_at___gt=now - timedelta(hours=24),
        )

    @patch("monitoring.alerts.s3_upload_failure_alert.RawFile")
    def test_get_issues_should_return_one_issue_per_failed_file(
        self, mock_raw_file: Mock
    ) -> None:
        """Each failed file yields an issue keyed by file id."""
        # given
        mock_raw_file.objects.filter.return_value.only.return_value = [
            _create_mock_raw_file(id="file1", project_id="P1", instrument_id="inst1"),
            _create_mock_raw_file(id="file2", project_id=None, instrument_id=None),
        ]
        alert = S3UploadFailureAlert()

        # when
        first = alert._get_issues([])
        second = alert._get_issues([])

        # then
        assert first == [
            Issue(
                "file1",
                "S3 upload failed | project: P1 | instrument: inst1",
                relevant_file="file1",
                instrument_id="inst1",
                unique_key=("file1",),
            ),
            Issue(
                "file2",
                "S3 upload failed | project: N/A | instrument: N/A",
                relevant_file="file2",
                instrument_id=None,
                unique_key=("file2",),
            ),
        ]
        assert second == first  # stateless: the alert manager reports each key once
