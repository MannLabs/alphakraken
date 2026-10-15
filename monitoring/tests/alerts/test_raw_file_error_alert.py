"""Unit tests for RawFileErrorAlert class."""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytz

from monitoring.alerts.base_alert import Issue
from monitoring.alerts.config import Cases
from monitoring.alerts.raw_file_error_alert import RawFileErrorAlert
from shared.db.models import RawFileStatus


class TestRawFileErrorAlert:
    """Test suite for RawFileErrorAlert class."""

    def test_case_name_should_return_correct_value(self) -> None:
        """Test that name property returns correct value."""
        # given
        alert = RawFileErrorAlert()

        # when
        result = alert.name

        # then
        assert result == Cases.RAW_FILE_ERROR

    def test_format_message_should_handle_comprehensive_scenarios(self) -> None:
        """Test that format_message handles multiple issues with various error details including None values."""
        # given
        alert = RawFileErrorAlert()

        # Complex mix of issues with different error details
        issues = [
            Issue("critical_file_001", "Database connection timeout after 30 seconds"),
            Issue(
                "instrument_data_corrupted",
                "Checksum mismatch - file appears corrupted",
            ),
            Issue("processing_job_123", None),
            Issue("network_file_transfer", "Network unreachable - connection refused"),
            Issue(
                "validation_error_456",
                "Schema validation failed: missing required fields",
            ),
            Issue("memory_issue_789", "Out of memory during processing"),
            Issue("disk_full_error", None),
        ]

        # when
        result = alert.format_message(issues)

        # then
        expected = (
            "Raw files changed to ERROR status:\n"
            "- `critical_file_001`: Database connection timeout after 30 seconds\n"
            "- `instrument_data_corrupted`: Checksum mismatch - file appears corrupted\n"
            "- `processing_job_123`: None\n"
            "- `network_file_transfer`: Network unreachable - connection refused\n"
            "- `validation_error_456`: Schema validation failed: missing required fields\n"
            "- `memory_issue_789`: Out of memory during processing\n"
            "- `disk_full_error`: None"
        )
        assert result == expected

    @patch("monitoring.alerts.raw_file_error_alert.RawFile")
    @patch("monitoring.alerts.raw_file_error_alert.datetime")
    @patch("monitoring.alerts.raw_file_error_alert.RAW_FILE_STATUS_LOOKBACK_HOURS", 24)
    def test_get_issues_should_query_recent_error_files(
        self, mock_datetime: Mock, mock_raw_file: Mock
    ) -> None:
        """Only files in ERROR status updated within the lookback are queried."""
        # given
        now = datetime(2024, 1, 2, 12, 0, 0, tzinfo=pytz.UTC)
        mock_datetime.now.return_value = now
        mock_raw_file.objects.filter.return_value.only.return_value = []

        # when
        result = RawFileErrorAlert()._get_issues([])

        # then
        assert result == []
        mock_raw_file.objects.filter.assert_called_once_with(
            status=RawFileStatus.ERROR, updated_at___gt=now - timedelta(hours=24)
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
        first = alert._get_issues([])
        second = alert._get_issues([])

        # then
        assert first == [
            Issue(
                "file1",
                "Disk full",
                relevant_file="file1",
                instrument_id="inst1",
                unique_key=("file1", "Disk full"),
            ),
            Issue(
                "file2",
                "None",
                relevant_file="file2",
                instrument_id="inst2",
                unique_key=("file2", "None"),
            ),
        ]
        assert second == first  # stateless: the alert manager reports each key once
