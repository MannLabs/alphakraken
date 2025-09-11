"""Unit tests for RawFileErrorAlert class."""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytz

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

    @patch("monitoring.alerts.raw_file_error_alert.RawFile")
    @patch("monitoring.alerts.raw_file_error_alert.datetime")
    @patch("monitoring.alerts.raw_file_error_alert.CHECK_INTERVAL_SECONDS", 60)
    def test_check_should_handle_comprehensive_error_scenarios(
        self, mock_datetime: Mock, mock_raw_file: Mock
    ) -> None:
        """Test that check handles complex mixed scenarios with various file status changes and error conditions."""
        # given
        alert = RawFileErrorAlert()
        fixed_now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=pytz.UTC)
        mock_datetime.now.return_value = fixed_now

        # Set up previous state with some files already in different statuses
        alert.previous_raw_file_statuses = {
            "file_already_error": RawFileStatus.ERROR,  # Already in error - should be ignored
            "file_was_done": RawFileStatus.DONE,  # Changed from DONE to ERROR - should trigger
            "file_was_quanting": RawFileStatus.QUANTING,  # Changed from QUANTING to ERROR - should trigger
            "file_stays_done": RawFileStatus.DONE,  # Still DONE - should not trigger
            "file_not_in_query": RawFileStatus.DONE,  # Not returned by query anymore
        }

        # Create comprehensive mix of files with different scenarios
        mock_file_already_error = Mock()
        mock_file_already_error.id = "file_already_error"
        mock_file_already_error.status = RawFileStatus.ERROR
        mock_file_already_error.status_details = "Still failing"

        mock_file_was_done = Mock()
        mock_file_was_done.id = "file_was_done"
        mock_file_was_done.status = RawFileStatus.ERROR
        mock_file_was_done.status_details = "Processing failed after completion"

        mock_file_was_quanting = Mock()
        mock_file_was_quanting.id = "file_was_quanting"
        mock_file_was_quanting.status = RawFileStatus.ERROR
        mock_file_was_quanting.status_details = "Failed during quanting"

        mock_file_stays_done = Mock()
        mock_file_stays_done.id = "file_stays_done"
        mock_file_stays_done.status = RawFileStatus.DONE
        mock_file_stays_done.status_details = "Completed successfully"

        mock_file_new_error = Mock()
        mock_file_new_error.id = "file_new_error"  # Not in previous state
        mock_file_new_error.status = RawFileStatus.ERROR
        mock_file_new_error.status_details = "New file encountered error"

        mock_file_new_done = Mock()
        mock_file_new_done.id = "file_new_done"  # Not in previous state
        mock_file_new_done.status = RawFileStatus.DONE
        mock_file_new_done.status_details = "New file completed"

        mock_file_error_no_details = Mock()
        mock_file_error_no_details.id = "file_error_no_details"
        mock_file_error_no_details.status = RawFileStatus.ERROR
        mock_file_error_no_details.status_details = None

        mock_file_quanting_to_done = Mock()
        mock_file_quanting_to_done.id = "file_quanting_to_done"
        mock_file_quanting_to_done.status = RawFileStatus.DONE
        mock_file_quanting_to_done.status_details = "Completed after quanting"

        mock_raw_file.objects.filter.return_value.only.return_value = [
            mock_file_already_error,
            mock_file_was_done,
            mock_file_was_quanting,
            mock_file_stays_done,
            mock_file_new_error,
            mock_file_new_done,
            mock_file_error_no_details,
            mock_file_quanting_to_done,
        ]

        # when
        result = alert.get_issues([])

        # then
        # Should only return files that changed TO error status (not already in error)
        expected = [
            ("file_was_done", "Processing failed after completion"),
            ("file_was_quanting", "Failed during quanting"),
            ("file_new_error", "New file encountered error"),
            ("file_error_no_details", "None"),
        ]
        assert result == expected

        # Verify the query was made with correct time window
        mock_raw_file.objects.filter.assert_called_once_with(
            updated_at___gt=fixed_now - timedelta(seconds=300)  # 60 * 5
        )

        # Verify previous_raw_file_statuses was updated correctly
        expected_previous_state = {
            "file_already_error": RawFileStatus.ERROR,
            "file_was_done": RawFileStatus.ERROR,
            "file_was_quanting": RawFileStatus.ERROR,
            "file_stays_done": RawFileStatus.DONE,
            "file_new_error": RawFileStatus.ERROR,
            "file_new_done": RawFileStatus.DONE,
            "file_error_no_details": RawFileStatus.ERROR,
            "file_quanting_to_done": RawFileStatus.DONE,
        }
        assert alert.previous_raw_file_statuses == expected_previous_state

    def test_format_message_should_handle_comprehensive_scenarios(self) -> None:
        """Test that format_message handles multiple issues with various error details including None values."""
        # given
        alert = RawFileErrorAlert()

        # Complex mix of issues with different error details
        issues = [
            ("critical_file_001", "Database connection timeout after 30 seconds"),
            ("instrument_data_corrupted", "Checksum mismatch - file appears corrupted"),
            ("processing_job_123", None),
            ("network_file_transfer", "Network unreachable - connection refused"),
            (
                "validation_error_456",
                "Schema validation failed: missing required fields",
            ),
            ("memory_issue_789", "Out of memory during processing"),
            ("disk_full_error", None),
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
    @patch("monitoring.alerts.raw_file_error_alert.CHECK_INTERVAL_SECONDS", 120)
    def test_check_should_return_empty_when_no_new_errors(
        self, mock_datetime: Mock, mock_raw_file: Mock
    ) -> None:
        """Test that check returns empty result when no files have newly changed to ERROR status."""
        # given
        alert = RawFileErrorAlert()
        fixed_now = datetime(2024, 1, 1, 15, 30, 0, tzinfo=pytz.UTC)
        mock_datetime.now.return_value = fixed_now

        # Set up previous state with files in various statuses
        alert.previous_raw_file_statuses = {
            "file1": RawFileStatus.ERROR,  # Already error
            "file2": RawFileStatus.DONE,  # Still done
            "file3": RawFileStatus.QUANTING,  # Still quanting
        }

        # Create files that maintain their previous status or aren't errors
        mock_file1 = Mock()
        mock_file1.id = "file1"
        mock_file1.status = RawFileStatus.ERROR  # Still error
        mock_file1.status_details = "Still failing"

        mock_file2 = Mock()
        mock_file2.id = "file2"
        mock_file2.status = RawFileStatus.DONE  # Still done
        mock_file2.status_details = "Completed"

        mock_file3 = Mock()
        mock_file3.id = "file3"
        mock_file3.status = RawFileStatus.QUANTING  # Still quanting
        mock_file3.status_details = "In progress"

        mock_file4 = Mock()
        mock_file4.id = "file4"  # New file, not error
        mock_file4.status = RawFileStatus.DONE
        mock_file4.status_details = "New completed file"

        mock_raw_file.objects.filter.return_value.only.return_value = [
            mock_file1,
            mock_file2,
            mock_file3,
            mock_file4,
        ]

        # when
        result = alert.get_issues([])

        # then
        assert result == []

        # Verify correct time window calculation
        mock_raw_file.objects.filter.assert_called_once_with(
            updated_at___gt=fixed_now - timedelta(seconds=600)  # 120 * 5
        )

    @patch("monitoring.alerts.raw_file_error_alert.RawFile")
    @patch("monitoring.alerts.raw_file_error_alert.datetime")
    @patch("monitoring.alerts.raw_file_error_alert.CHECK_INTERVAL_SECONDS", 90)
    def test_state_tracking_and_time_window_behavior(
        self, mock_datetime: Mock, mock_raw_file: Mock
    ) -> None:
        """Test that state tracking works correctly and time window is calculated properly."""
        # given
        alert = RawFileErrorAlert()
        fixed_now = datetime(2024, 2, 15, 9, 45, 0, tzinfo=pytz.UTC)
        mock_datetime.now.return_value = fixed_now

        # Empty previous state (first run)
        assert alert.previous_raw_file_statuses == {}

        # Create files with various statuses
        mock_error_file = Mock()
        mock_error_file.id = "first_error"
        mock_error_file.status = RawFileStatus.ERROR
        mock_error_file.status_details = "First time error"

        mock_done_file = Mock()
        mock_done_file.id = "completed_file"
        mock_done_file.status = RawFileStatus.DONE
        mock_done_file.status_details = "Successfully completed"

        mock_raw_file.objects.filter.return_value.only.return_value = [
            mock_error_file,
            mock_done_file,
        ]

        # when - first check
        result1 = alert.get_issues([])

        # then - first check should detect new error
        assert result1 == [("first_error", "First time error")]

        # Verify correct time window for first check
        expected_time_window = fixed_now - timedelta(seconds=450)  # 90 * 5
        mock_raw_file.objects.filter.assert_called_with(
            updated_at___gt=expected_time_window
        )

        # Verify state was updated
        assert alert.previous_raw_file_statuses == {
            "first_error": RawFileStatus.ERROR,
            "completed_file": RawFileStatus.DONE,
        }

        # Reset mock for second check
        mock_raw_file.reset_mock()

        # Setup for second check - same files, no status changes
        mock_raw_file.objects.filter.return_value.only.return_value = [
            mock_error_file,
            mock_done_file,
        ]

        # when - second check
        result2 = alert.get_issues([])

        # then - second check should find no new errors
        assert result2 == []
