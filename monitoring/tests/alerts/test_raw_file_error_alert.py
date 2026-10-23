"""Unit tests for RawFileErrorAlert class."""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytz

from monitoring.alerts.base_alert import Issue
from monitoring.alerts.raw_file_error_alert import RawFileErrorAlert
from shared.db.models import RawFileStatus

NOW = datetime(2024, 1, 2, 12, 0, 0, tzinfo=pytz.UTC)


class TestRawFileErrorAlert:
    """Test suite for RawFileErrorAlert class."""

    def test_name_should_return_class_name(self) -> None:
        """Test that name property returns the class name."""
        assert RawFileErrorAlert().name == "RawFileErrorAlert"

    def test_is_reported_once_per_file_and_error(self) -> None:
        """Each file and error is reported once, without cooldown."""
        assert RawFileErrorAlert().cooldown_minutes is None

    def test_format_message_should_handle_none_details(self) -> None:
        """Test that format_message lists issues with and without details."""
        # given
        alert = RawFileErrorAlert()
        issues = [
            Issue("critical_file_001", "Database connection timeout after 30 seconds"),
            Issue("processing_job_123", None),
        ]

        # when
        result = alert.format_message(issues)

        # then
        assert result == (
            "- `critical_file_001`: Database connection timeout after 30 seconds\n"
            "- `processing_job_123`: None"
        )

    @patch("monitoring.alerts.raw_file_error_alert.RawFile")
    @patch("monitoring.alerts.raw_file_error_alert.RAW_FILE_STATUS_LOOKBACK_HOURS", 24)
    def test_get_issues_should_query_recent_error_files(
        self, mock_raw_file: Mock
    ) -> None:
        """Only files in ERROR status updated within the lookback are queried."""
        # given
        mock_raw_file.objects.filter.return_value.only.return_value = []

        # when
        result = RawFileErrorAlert()._get_issues([], NOW)

        # then
        assert result == []
        mock_raw_file.objects.filter.assert_called_once_with(
            status=RawFileStatus.ERROR, updated_at___gt=NOW - timedelta(hours=24)
        )

    @patch("monitoring.alerts.raw_file_error_alert.RawFile")
    def test_get_issues_should_return_one_issue_per_error_file(
        self, mock_raw_file: Mock
    ) -> None:
        """Each error file yields an issue keyed by file id and error details."""
        # given
        mock_raw_file.objects.filter.return_value.only.return_value = [
            Mock(id="file1", status_details="Disk full", instrument_id="inst1"),
            Mock(id="file2", status_details=None, instrument_id="inst2"),
        ]
        alert = RawFileErrorAlert()

        # when
        first = alert._get_issues([], NOW)
        second = alert._get_issues([], NOW)

        # then
        assert [
            (i.identifier, i.details, i.relevant_file, i.instrument_id) for i in first
        ] == [
            ("file1", "Disk full", "file1", "inst1"),
            ("file2", "None", "file2", "inst2"),
        ]
        assert first[0].key.startswith("file1:")
        assert first[0].key != first[1].key
        assert second == first  # stateless: the alert manager reports each key once

    @patch("monitoring.alerts.raw_file_error_alert.RawFile")
    def test_key_changes_with_the_error_details(self, mock_raw_file: Mock) -> None:
        """A file failing again with a different error gets a new key."""
        # given
        alert = RawFileErrorAlert()
        mock_raw_file.objects.filter.return_value.only.return_value = [
            Mock(id="file1", status_details="first error", instrument_id="inst1")
        ]
        first = alert._get_issues([], NOW)[0]
        mock_raw_file.objects.filter.return_value.only.return_value = [
            Mock(id="file1", status_details="second error", instrument_id="inst1")
        ]

        # when
        second = alert._get_issues([], NOW)[0]

        # then
        assert first.key != second.key
