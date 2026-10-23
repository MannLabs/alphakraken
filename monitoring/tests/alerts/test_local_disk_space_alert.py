"""Unit tests for LocalDiskSpaceAlert class."""

from datetime import datetime
from unittest.mock import Mock, patch

import pytz

from monitoring.alerts import config
from monitoring.alerts.base_alert import Issue
from monitoring.alerts.local_disk_space_alert import LocalDiskSpaceAlert

NOW = datetime(2024, 1, 15, 12, 0, tzinfo=pytz.UTC)


class TestLocalDiskSpaceAlert:
    """Test suite for LocalDiskSpaceAlert class."""

    def test_name_should_return_local_disk_space_case(self) -> None:
        """Test that name property returns correct value."""
        # given
        alert = LocalDiskSpaceAlert()

        # when
        result = alert.name

        # then
        assert result == "LocalDiskSpaceAlert"

    @patch("monitoring.alerts.local_disk_space_alert.shutil.disk_usage")
    def test_get_issues_should_return_empty_list_when_above_threshold(
        self, mock_disk_usage: Mock
    ) -> None:
        """Test that get_issues returns empty list when free space is above threshold."""
        # given
        alert = LocalDiskSpaceAlert()
        mock_disk_usage.return_value = Mock(free=50 * config.BYTES_PER_GB)

        # when
        result = alert._get_issues([], NOW)

        # then
        assert result == []
        mock_disk_usage.assert_called_once_with(config.LOCAL_DISK_PATH)

    @patch("monitoring.alerts.local_disk_space_alert.shutil.disk_usage")
    def test_get_issues_should_return_issue_when_below_threshold(
        self, mock_disk_usage: Mock
    ) -> None:
        """Test that get_issues returns an issue when free space is below threshold."""
        # given
        alert = LocalDiskSpaceAlert()
        mock_disk_usage.return_value = Mock(free=5 * config.BYTES_PER_GB)

        # when
        result = alert._get_issues([], NOW)

        # then
        assert result == [Issue("local", 5)]

    @patch("monitoring.alerts.local_disk_space_alert.shutil.disk_usage")
    def test_get_issues_should_return_empty_list_when_exactly_at_threshold(
        self, mock_disk_usage: Mock
    ) -> None:
        """Test that get_issues does not alert when free space equals the threshold."""
        # given
        alert = LocalDiskSpaceAlert()
        mock_disk_usage.return_value = Mock(
            free=config.LOCAL_FREE_SPACE_THRESHOLD_GB * config.BYTES_PER_GB
        )

        # when
        result = alert._get_issues([], NOW)

        # then
        assert result == []

    @patch("monitoring.alerts.local_disk_space_alert.shutil.disk_usage")
    def test_get_issues_should_return_empty_list_when_disk_usage_raises(
        self, mock_disk_usage: Mock
    ) -> None:
        """Test that get_issues returns empty list when disk usage cannot be read."""
        # given
        alert = LocalDiskSpaceAlert()
        mock_disk_usage.side_effect = OSError("no such path")

        # when
        result = alert._get_issues([], NOW)

        # then
        assert result == []

    def test_format_message_should_format_issue_correctly(self) -> None:
        """Test that format_message formats the issue correctly."""
        # given
        alert = LocalDiskSpaceAlert()
        issues = [Issue("local", 5)]

        # when
        result = alert.format_message(issues)

        # then
        expected = f"{config.LOCAL_DISK_PATH}: 5 GB free"
        assert result == expected
