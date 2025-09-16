"""Unit tests for DiskSpaceAlert class."""

from unittest.mock import Mock, patch

from monitoring.alerts.config import Cases
from monitoring.alerts.disk_space_alert import DiskSpaceAlert
from shared.db.models import KrakenStatusEntities


class TestDiskSpaceAlert:
    """Test suite for DiskSpaceAlert class."""

    def test_name_should_return_low_disk_space_case(self) -> None:
        """Test that name property returns correct value."""
        # given
        alert = DiskSpaceAlert()

        # when
        result = alert.name

        # then
        assert result == Cases.LOW_DISK_SPACE

    @patch.object(DiskSpaceAlert, "_get_threshold")
    def test_get_issues_should_return_empty_list_when_no_low_disk_space(
        self, mock_get_threshold: Mock
    ) -> None:
        """Test that get_issues returns empty list when no disk space issues."""
        # given
        alert = DiskSpaceAlert()
        mock_get_threshold.return_value = 200

        mock_status1 = Mock()
        mock_status1.id = "instrument1"
        mock_status1.entity_type = KrakenStatusEntities.INSTRUMENT
        mock_status1.free_space_gb = 250

        mock_status2 = Mock()
        mock_status2.id = "backup"
        mock_status2.entity_type = KrakenStatusEntities.FILE_SYSTEM
        mock_status2.free_space_gb = 300

        status_objects = [mock_status1, mock_status2]

        # when
        result = alert._get_issues(status_objects)

        # then
        assert result == []

    @patch.object(DiskSpaceAlert, "_get_threshold")
    def test_get_issues_should_return_multiple_issues_when_multiple_low_disk_space(
        self, mock_get_threshold: Mock
    ) -> None:
        """Test that get_issues returns multiple issues when multiple entities have low disk space, and not consider jobs."""
        # given
        alert = DiskSpaceAlert()
        mock_get_threshold.return_value = 200

        mock_status1 = Mock()
        mock_status1.id = "instrument1"
        mock_status1.entity_type = KrakenStatusEntities.INSTRUMENT
        mock_status1.free_space_gb = 150

        mock_status2 = Mock()
        mock_status2.id = "backup"
        mock_status2.entity_type = KrakenStatusEntities.FILE_SYSTEM
        mock_status2.free_space_gb = 180

        mock_status3 = Mock()
        mock_status3.id = "instrument2"
        mock_status3.entity_type = KrakenStatusEntities.INSTRUMENT
        mock_status3.free_space_gb = 100

        mock_job = Mock()
        mock_job.id = "file_remover"
        mock_job.entity_type = KrakenStatusEntities.JOB
        mock_job.free_space_gb = 50

        mock_status4 = Mock()
        mock_status4.id = "output"
        mock_status4.entity_type = KrakenStatusEntities.FILE_SYSTEM
        mock_status4.free_space_gb = 30000

        status_objects = [
            mock_status1,
            mock_status2,
            mock_status3,
            mock_job,
            mock_status4,
        ]

        # when
        result = alert._get_issues(status_objects)

        # then
        assert result == [("instrument1", 150), ("backup", 180), ("instrument2", 100)]

    @patch.object(DiskSpaceAlert, "_get_threshold")
    def test_get_issues_should_return_mixed_results_when_mixed_disk_space_levels(
        self, mock_get_threshold: Mock
    ) -> None:
        """Test that get_issues returns only low disk space issues in mixed scenario."""
        # given
        alert = DiskSpaceAlert()
        mock_get_threshold.return_value = 200

        mock_status1 = Mock()
        mock_status1.id = "instrument1"
        mock_status1.entity_type = KrakenStatusEntities.INSTRUMENT
        mock_status1.free_space_gb = 250  # OK

        mock_status2 = Mock()
        mock_status2.id = "backup"
        mock_status2.entity_type = KrakenStatusEntities.FILE_SYSTEM
        mock_status2.free_space_gb = 150  # Low

        mock_status3 = Mock()
        mock_status3.id = "instrument2"
        mock_status3.entity_type = KrakenStatusEntities.INSTRUMENT
        mock_status3.free_space_gb = 300  # OK

        mock_status4 = Mock()
        mock_status4.id = "output"
        mock_status4.entity_type = KrakenStatusEntities.FILE_SYSTEM
        mock_status4.free_space_gb = 180  # Low

        status_objects = [mock_status1, mock_status2, mock_status3, mock_status4]

        # when
        result = alert._get_issues(status_objects)

        # then
        assert result == [("backup", 150), ("output", 180)]

    def test_format_message_should_format_single_issue_correctly(self) -> None:
        """Test that format_message formats single issue correctly."""
        # given
        alert = DiskSpaceAlert()
        issues = [("instrument1", 150)]

        # when
        result = alert.format_message(issues)

        # then
        expected = "Low disk space detected:\n- `instrument1`: 150 GB"
        assert result == expected

    def test_format_message_should_format_multiple_issues_correctly(self) -> None:
        """Test that format_message formats multiple issues correctly."""
        # given
        alert = DiskSpaceAlert()
        issues = [
            ("instrument1", 150),
            ("backup", 180),
            ("output", 100),
        ]

        # when
        result = alert.format_message(issues)

        # then
        expected = (
            "Low disk space detected:\n"
            "- `instrument1`: 150 GB\n"
            "- `backup`: 180 GB\n"
            "- `output`: 100 GB"
        )
        assert result == expected
