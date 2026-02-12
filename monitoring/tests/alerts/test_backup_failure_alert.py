"""Unit tests for BackupFailureAlert class."""

from datetime import datetime, timedelta
from unittest.mock import Mock, patch

import pytz

from monitoring.alerts.backup_failure_alert import BackupFailureAlert
from monitoring.alerts.config import Cases
from shared.db.models import BackupStatus


def _create_mock_raw_file(**kwargs: str | None) -> Mock:
    """Create a mock RawFile with the given attributes."""
    mock_file = Mock()
    for key, value in kwargs.items():
        setattr(mock_file, key, value)
    return mock_file


class TestBackupFailureAlert:
    """Test suite for BackupFailureAlert class."""

    def test_case_name_should_return_correct_value(self) -> None:
        """Test that name property returns correct value."""
        # given
        alert = BackupFailureAlert()

        # when
        result = alert.name

        # then
        assert result == Cases.BACKUP_FAILURE

    @patch("monitoring.alerts.backup_failure_alert.RawFile")
    @patch("monitoring.alerts.backup_failure_alert.datetime")
    @patch("monitoring.alerts.backup_failure_alert.CHECK_INTERVAL_SECONDS", 60)
    def test_check_should_detect_new_copying_failed_transition(
        self, mock_datetime: Mock, mock_raw_file: Mock
    ) -> None:
        """Test that check detects new COPYING_FAILED transition."""
        # given
        alert = BackupFailureAlert()
        fixed_now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=pytz.UTC)
        mock_datetime.now.return_value = fixed_now

        alert.previous_backup_statuses = {
            "file_was_copying": BackupStatus.COPYING_IN_PROGRESS,
        }

        mock_file = Mock()
        mock_file.id = "file_was_copying"
        mock_file.backup_status = BackupStatus.COPYING_FAILED
        mock_file.project_id = "proj1"
        mock_file.instrument_id = "inst1"
        mock_file.backup_base_path = "/backup/path"
        mock_file.s3_upload_path = None

        mock_raw_file.objects.filter.return_value.only.return_value = [mock_file]

        # when
        result = alert._get_issues([])

        # then
        assert len(result) == 1
        assert result[0][0] == "file_was_copying"
        assert "Copying to backup failed" in result[0][1]
        assert "/backup/path" in result[0][1]
        assert "proj1" in result[0][1]
        assert "inst1" in result[0][1]

    @patch("monitoring.alerts.backup_failure_alert.RawFile")
    @patch("monitoring.alerts.backup_failure_alert.datetime")
    @patch("monitoring.alerts.backup_failure_alert.CHECK_INTERVAL_SECONDS", 60)
    def test_check_should_detect_new_upload_failed_transition(
        self, mock_datetime: Mock, mock_raw_file: Mock
    ) -> None:
        """Test that check detects new UPLOAD_FAILED transition."""
        # given
        alert = BackupFailureAlert()
        fixed_now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=pytz.UTC)
        mock_datetime.now.return_value = fixed_now

        alert.previous_backup_statuses = {
            "file_was_uploading": BackupStatus.UPLOAD_IN_PROGRESS,
        }

        mock_file = Mock()
        mock_file.id = "file_was_uploading"
        mock_file.backup_status = BackupStatus.UPLOAD_FAILED
        mock_file.project_id = "proj2"
        mock_file.instrument_id = "inst2"
        mock_file.backup_base_path = "/backup/path"
        mock_file.s3_upload_path = "s3://bucket/key"

        mock_raw_file.objects.filter.return_value.only.return_value = [mock_file]

        # when
        result = alert._get_issues([])

        # then
        assert len(result) == 1
        assert result[0][0] == "file_was_uploading"
        assert "S3 upload failed" in result[0][1]
        assert "s3://bucket/key" in result[0][1]
        assert "proj2" in result[0][1]
        assert "inst2" in result[0][1]

    @patch("monitoring.alerts.backup_failure_alert.RawFile")
    @patch("monitoring.alerts.backup_failure_alert.datetime")
    @patch("monitoring.alerts.backup_failure_alert.CHECK_INTERVAL_SECONDS", 60)
    def test_check_should_ignore_files_already_in_failed_state(
        self, mock_datetime: Mock, mock_raw_file: Mock
    ) -> None:
        """Test that check ignores files already in failed state."""
        # given
        alert = BackupFailureAlert()
        fixed_now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=pytz.UTC)
        mock_datetime.now.return_value = fixed_now

        alert.previous_backup_statuses = {
            "file_already_failed": BackupStatus.COPYING_FAILED,
        }

        mock_file = Mock()
        mock_file.id = "file_already_failed"
        mock_file.backup_status = BackupStatus.COPYING_FAILED
        mock_file.project_id = "proj1"
        mock_file.instrument_id = "inst1"
        mock_file.backup_base_path = "/backup/path"
        mock_file.s3_upload_path = None

        mock_raw_file.objects.filter.return_value.only.return_value = [mock_file]

        # when
        result = alert._get_issues([])

        # then
        assert result == []

    @patch("monitoring.alerts.backup_failure_alert.RawFile")
    @patch("monitoring.alerts.backup_failure_alert.datetime")
    @patch("monitoring.alerts.backup_failure_alert.CHECK_INTERVAL_SECONDS", 60)
    def test_check_should_handle_mixed_scenarios(
        self, mock_datetime: Mock, mock_raw_file: Mock
    ) -> None:
        """Test that check handles complex mixed scenarios correctly."""
        # given
        alert = BackupFailureAlert()
        fixed_now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=pytz.UTC)
        mock_datetime.now.return_value = fixed_now

        alert.previous_backup_statuses = {
            "file_already_copy_failed": BackupStatus.COPYING_FAILED,
            "file_was_copying": BackupStatus.COPYING_IN_PROGRESS,
            "file_was_uploading": BackupStatus.UPLOAD_IN_PROGRESS,
            "file_copy_done": BackupStatus.COPYING_DONE,
        }

        mock_raw_file.objects.filter.return_value.only.return_value = [
            _create_mock_raw_file(
                id="file_already_copy_failed",
                backup_status=BackupStatus.COPYING_FAILED,
                project_id="proj1",
                instrument_id="inst1",
                backup_base_path="/backup/path1",
            ),
            _create_mock_raw_file(
                id="file_was_copying",
                backup_status=BackupStatus.COPYING_FAILED,
                project_id="proj2",
                instrument_id="inst2",
                backup_base_path="/backup/path2",
            ),
            _create_mock_raw_file(
                id="file_was_uploading",
                backup_status=BackupStatus.UPLOAD_FAILED,
                project_id="proj3",
                instrument_id="inst3",
                backup_base_path="/backup/path3",
                s3_upload_path="s3://bucket/key3",
            ),
            _create_mock_raw_file(
                id="file_copy_done",
                backup_status=BackupStatus.UPLOAD_DONE,
                project_id="proj4",
                instrument_id="inst4",
                backup_base_path="/backup/path4",
                s3_upload_path="s3://bucket/key4",
            ),
            _create_mock_raw_file(
                id="file_new_failed",
                backup_status=BackupStatus.UPLOAD_FAILED,
                project_id="proj5",
                instrument_id="inst5",
                backup_base_path="/backup/path5",
                s3_upload_path="s3://bucket/key5",
            ),
        ]

        # when
        result = alert._get_issues([])

        # then
        assert len(result) == 3
        result_ids = [r[0] for r in result]
        assert "file_was_copying" in result_ids
        assert "file_was_uploading" in result_ids
        assert "file_new_failed" in result_ids
        assert "file_already_copy_failed" not in result_ids
        assert "file_copy_done" not in result_ids

        mock_raw_file.objects.filter.assert_called_once_with(
            updated_at___gt=fixed_now - timedelta(seconds=300)
        )

        expected_previous_state = {
            "file_already_copy_failed": BackupStatus.COPYING_FAILED,
            "file_was_copying": BackupStatus.COPYING_FAILED,
            "file_was_uploading": BackupStatus.UPLOAD_FAILED,
            "file_copy_done": BackupStatus.UPLOAD_DONE,
            "file_new_failed": BackupStatus.UPLOAD_FAILED,
        }
        assert alert.previous_backup_statuses == expected_previous_state

    def test_format_message_should_format_correctly(self) -> None:
        """Test that format_message produces correct output."""
        # given
        alert = BackupFailureAlert()
        issues = [
            (
                "file1",
                "Copying to backup failed | path: /backup/path | project: proj1 | instrument: inst1",
            ),
            (
                "file2",
                "S3 upload failed | path: s3://bucket/key | project: proj2 | instrument: inst2",
            ),
        ]

        # when
        result = alert.format_message(issues)

        # then
        expected = (
            "Backup failures detected:\n"
            "- `file1`: Copying to backup failed | path: /backup/path | project: proj1 | instrument: inst1\n"
            "- `file2`: S3 upload failed | path: s3://bucket/key | project: proj2 | instrument: inst2"
        )
        assert result == expected

    @patch("monitoring.alerts.backup_failure_alert.RawFile")
    @patch("monitoring.alerts.backup_failure_alert.datetime")
    @patch("monitoring.alerts.backup_failure_alert.CHECK_INTERVAL_SECONDS", 60)
    def test_check_should_handle_missing_fields(
        self, mock_datetime: Mock, mock_raw_file: Mock
    ) -> None:
        """Test that check handles missing optional fields gracefully."""
        # given
        alert = BackupFailureAlert()
        fixed_now = datetime(2024, 1, 1, 12, 0, 0, tzinfo=pytz.UTC)
        mock_datetime.now.return_value = fixed_now

        mock_file = Mock()
        mock_file.id = "file_missing_fields"
        mock_file.backup_status = BackupStatus.COPYING_FAILED
        mock_file.project_id = None
        mock_file.instrument_id = None
        mock_file.backup_base_path = None
        mock_file.s3_upload_path = None

        mock_raw_file.objects.filter.return_value.only.return_value = [mock_file]

        # when
        result = alert._get_issues([])

        # then
        assert len(result) == 1
        assert result[0][0] == "file_missing_fields"
        assert "N/A" in result[0][1]
