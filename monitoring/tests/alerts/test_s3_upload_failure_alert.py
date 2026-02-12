"""Unit tests for S3UploadFailureAlert class."""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytz

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

    @patch("monitoring.alerts.s3_upload_failure_alert.RawFile")
    @patch("monitoring.alerts.s3_upload_failure_alert.datetime")
    @patch("monitoring.alerts.s3_upload_failure_alert.CHECK_INTERVAL_SECONDS", 60)
    def test_check_should_detect_new_upload_failed_transition(
        self, mock_datetime: Mock, mock_raw_file: Mock
    ) -> None:
        """Test that check detects new UPLOAD_FAILED transition."""
        # given
        alert = S3UploadFailureAlert()
        fixed_now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=pytz.UTC)
        mock_datetime.now.return_value = fixed_now

        alert.previous_upload_statuses = {
            "file_was_uploading": BackupStatus.UPLOAD_IN_PROGRESS,
        }

        mock_file = Mock()
        mock_file.id = "file_was_uploading"
        mock_file.backup_status = BackupStatus.UPLOAD_FAILED
        mock_file.project_id = "proj2"
        mock_file.instrument_id = "inst2"

        mock_raw_file.objects.filter.return_value.only.return_value = [mock_file]

        # when
        result = alert._get_issues([])

        # then
        assert len(result) == 1
        assert result[0][0] == "file_was_uploading"
        assert "S3 upload failed" in result[0][1]
        assert "proj2" in result[0][1]
        assert "inst2" in result[0][1]

    @patch("monitoring.alerts.s3_upload_failure_alert.RawFile")
    @patch("monitoring.alerts.s3_upload_failure_alert.datetime")
    @patch("monitoring.alerts.s3_upload_failure_alert.CHECK_INTERVAL_SECONDS", 60)
    def test_check_should_ignore_files_already_in_failed_state(
        self, mock_datetime: Mock, mock_raw_file: Mock
    ) -> None:
        """Test that check ignores files already in failed state."""
        # given
        alert = S3UploadFailureAlert()
        fixed_now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=pytz.UTC)
        mock_datetime.now.return_value = fixed_now

        alert.previous_upload_statuses = {
            "file_already_failed": BackupStatus.UPLOAD_FAILED,
        }

        mock_file = Mock()
        mock_file.id = "file_already_failed"
        mock_file.backup_status = BackupStatus.UPLOAD_FAILED
        mock_file.project_id = "proj1"
        mock_file.instrument_id = "inst1"
        mock_file.s3_upload_path = "s3://bucket/key"

        mock_raw_file.objects.filter.return_value.only.return_value = [mock_file]

        # when
        result = alert._get_issues([])

        # then
        assert result == []

    @patch("monitoring.alerts.s3_upload_failure_alert.RawFile")
    @patch("monitoring.alerts.s3_upload_failure_alert.datetime")
    @patch("monitoring.alerts.s3_upload_failure_alert.CHECK_INTERVAL_SECONDS", 60)
    def test_check_should_handle_mixed_scenarios(
        self, mock_datetime: Mock, mock_raw_file: Mock
    ) -> None:
        """Test that check handles complex mixed scenarios correctly."""
        # given
        alert = S3UploadFailureAlert()
        fixed_now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=pytz.UTC)
        mock_datetime.now.return_value = fixed_now

        alert.previous_upload_statuses = {
            "file_already_upload_failed": BackupStatus.UPLOAD_FAILED,
            "file_was_uploading": BackupStatus.UPLOAD_IN_PROGRESS,
            "file_copy_done": BackupStatus.COPYING_DONE,
        }

        mock_raw_file.objects.filter.return_value.only.return_value = [
            _create_mock_raw_file(
                id="file_already_upload_failed",
                backup_status=BackupStatus.UPLOAD_FAILED,
                project_id="proj1",
                instrument_id="inst1",
                s3_upload_path="s3://bucket/key1",
            ),
            _create_mock_raw_file(
                id="file_was_uploading",
                backup_status=BackupStatus.UPLOAD_FAILED,
                project_id="proj2",
                instrument_id="inst2",
                s3_upload_path="s3://bucket/key2",
            ),
            _create_mock_raw_file(
                id="file_copy_done",
                backup_status=BackupStatus.UPLOAD_DONE,
                project_id="proj3",
                instrument_id="inst3",
                s3_upload_path="s3://bucket/key3",
            ),
            _create_mock_raw_file(
                id="file_new_failed",
                backup_status=BackupStatus.UPLOAD_FAILED,
                project_id="proj4",
                instrument_id="inst4",
                s3_upload_path="s3://bucket/key4",
            ),
        ]

        # when
        result = alert._get_issues([])

        # then
        assert len(result) == 2
        result_ids = [r[0] for r in result]
        assert "file_was_uploading" in result_ids
        assert "file_new_failed" in result_ids
        assert "file_already_upload_failed" not in result_ids
        assert "file_copy_done" not in result_ids

        mock_raw_file.objects.filter.assert_called_once_with(
            updated_at___gt=fixed_now - timedelta(seconds=300)
        )

        expected_previous_state = {
            "file_already_upload_failed": BackupStatus.UPLOAD_FAILED,
            "file_was_uploading": BackupStatus.UPLOAD_FAILED,
            "file_copy_done": BackupStatus.UPLOAD_DONE,
            "file_new_failed": BackupStatus.UPLOAD_FAILED,
        }
        assert alert.previous_upload_statuses == expected_previous_state

    def test_format_message_should_format_correctly(self) -> None:
        """Test that format_message produces correct output."""
        # given
        alert = S3UploadFailureAlert()
        issues = [
            (
                "file1",
                "S3 upload failed | path: s3://bucket/key1 | project: proj1 | instrument: inst1",
            ),
            (
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
    @patch("monitoring.alerts.s3_upload_failure_alert.CHECK_INTERVAL_SECONDS", 60)
    def test_check_should_handle_missing_fields(
        self, mock_datetime: Mock, mock_raw_file: Mock
    ) -> None:
        """Test that check handles missing optional fields gracefully."""
        # given
        alert = S3UploadFailureAlert()
        fixed_now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=pytz.UTC)
        mock_datetime.now.return_value = fixed_now

        mock_file = Mock()
        mock_file.id = "file_missing_fields"
        mock_file.backup_status = BackupStatus.UPLOAD_FAILED
        mock_file.project_id = None
        mock_file.instrument_id = None
        mock_file.s3_upload_path = None

        mock_raw_file.objects.filter.return_value.only.return_value = [mock_file]

        # when
        result = alert._get_issues([])

        # then
        assert len(result) == 1
        assert result[0][0] == "file_missing_fields"
        assert "N/A" in result[0][1]
