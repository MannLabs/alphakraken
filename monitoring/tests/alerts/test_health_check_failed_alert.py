"""Unit tests for  HealthCheckFailedAlert class."""

from unittest.mock import Mock

from monitoring.alerts.config import Cases
from monitoring.alerts.health_check_failed_alert import HealthCheckFailedAlert
from shared.db.models import KrakenStatusValues


class TestHealthCheckFailedAlert:
    """Test suite for  HealthCheckFailedAlert class."""

    def test_name_should_return_health_check_failed_case(self) -> None:
        """Test that name property returns correct value."""
        # given
        alert = HealthCheckFailedAlert()

        # when
        result = alert.name

        # then
        assert result == Cases.HEALTH_CHECK_FAILED

    def test_get_issues_should_return_empty_list_when_all_status_ok(self) -> None:
        """Test that get_issues returns empty list when all health checks are OK."""
        # given
        alert = HealthCheckFailedAlert()
        mock_status1 = Mock()
        mock_status1.id = "instrument1"
        mock_status1.status = KrakenStatusValues.OK
        mock_status1.status_details = "All systems operational"

        status_objects = [mock_status1]

        # when
        result = alert.get_issues(status_objects)

        # then
        assert result == []

    def test_get_issues_should_return_multiple_issues_when_multiple_health_checks_failed(
        self,
    ) -> None:
        """Test that get_issues returns multiple issues when multiple health checks fail."""
        # given
        alert = HealthCheckFailedAlert()
        mock_status1 = Mock()
        mock_status1.id = "instrument1"
        mock_status1.status = KrakenStatusValues.ERROR
        mock_status1.status_details = "Disk full"

        mock_status2 = Mock()
        mock_status2.id = "instrument2"
        mock_status2.status = KrakenStatusValues.OK
        mock_status2.status_details = ""

        mock_status3 = Mock()
        mock_status3.id = "instrument3"
        mock_status3.status = KrakenStatusValues.ERROR
        mock_status3.status_details = "Service unavailable"

        status_objects = [mock_status1, mock_status2, mock_status3]

        # when
        result = alert.get_issues(status_objects)

        # then
        assert result == [
            ("instrument1", "Disk full"),
            ("instrument3", "Service unavailable"),
        ]

    def test_format_message_should_format_multiple_issues_correctly(self) -> None:
        """Test that format_message formats multiple issues correctly."""
        # given
        alert = HealthCheckFailedAlert()
        issues = [
            ("instrument1", "Disk full"),
            ("instrument2", "Connection timeout"),
            ("instrument3", "Service unavailable"),
        ]

        # when
        result = alert.format_message(issues)

        # then
        expected = (
            "Health check failed:\n"
            "- `instrument1`: Disk full\n"
            "- `instrument2`: Connection timeout\n"
            "- `instrument3`: Service unavailable"
        )
        assert result == expected

    def test_format_message_should_handle_none_status_details_in_issues(self) -> None:
        """Test that format_message handles None status details in issues."""
        # given
        alert = HealthCheckFailedAlert()
        issues = [("instrument1", None), ("instrument2", "Connection timeout")]

        # when
        result = alert.format_message(issues)

        # then
        expected = (
            "Health check failed:\n"
            "- `instrument1`: None\n"
            "- `instrument2`: Connection timeout"
        )
        assert result == expected
