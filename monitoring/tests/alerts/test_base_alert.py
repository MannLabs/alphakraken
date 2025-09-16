"""Unit tests for BaseAlert suppression functionality."""

from unittest.mock import patch

from monitoring.alerts.base_alert import BaseAlert


class TestAlert(BaseAlert):
    """Test implementation of BaseAlert for testing purposes."""

    def __init__(self) -> None:
        """Initialize the TestAlert with no issues."""
        super().__init__()
        self.test_issues: list[tuple] = []

    @property
    def name(self) -> str:
        """Return the name of the alert."""
        return "test_alert"

    def _get_issues(
        self,
        status_objects: list,  # noqa: ARG002
    ) -> list[tuple]:
        """Mock implementation to return predefined issues."""
        # Return predefined issues for testing
        return self.test_issues if hasattr(self, "test_issues") else []

    def format_message(self, issues: list[tuple]) -> str:
        """Format the alert message for the issues found."""
        return f"Test message with {len(issues)} issues"


class TestBaseAlertSuppressions:
    """Test suite for BaseAlert suppression functionality."""

    def test_no_suppressions_file_returns_all_issues(self) -> None:
        """Test that when no suppressions file exists, all issues are returned."""
        # given
        alert = TestAlert()
        alert.test_issues = [
            ("instrument1", "error message"),
            ("instrument2", "another error"),
        ]

        with patch("monitoring.alerts.base_alert._suppressions", []):
            # when
            result = alert.get_issues([])

            # then
            assert result == [
                ("instrument1", "error message"),
                ("instrument2", "another error"),
            ]

    def test_exact_context_and_message_suppression(self) -> None:
        """Test exact context and message matching suppression."""
        # given
        alert = TestAlert()
        alert.test_issues = [
            ("instrument1", "copy failed"),
            ("instrument2", "disk full"),
        ]

        suppressions = [
            {
                "error_class": "TestAlert",
                "context": "instrument1",
                "message": "copy failed",
            }
        ]

        with patch("monitoring.alerts.base_alert._suppressions", suppressions):
            # when
            result = alert.get_issues([])

            # then
            assert result == [("instrument2", "disk full")]

    def test_wildcard_context_suppresses_all_contexts(self) -> None:
        """Test that context: '*' suppresses all contexts for the error class."""
        # given
        alert = TestAlert()
        alert.test_issues = [
            ("instrument1", "disk full"),
            ("instrument2", "disk full"),
            ("backup_system", "disk full"),
        ]

        suppressions = [
            {"error_class": "TestAlert", "context": "*", "message": "disk full"}
        ]

        with patch("monitoring.alerts.base_alert._suppressions", suppressions):
            # when
            result = alert.get_issues([])

            # then
            assert result == []

    def test_multiple_suppression_rules(self) -> None:
        """Test that multiple suppression rules are all applied."""
        # given
        alert = TestAlert()
        alert.test_issues = [
            ("instrument1", "copy failed"),
            ("instrument2", "disk full"),
            ("instrument3", "network error"),
            ("instrument4", "validation error"),
        ]

        suppressions = [
            {
                "error_class": "TestAlert",
                "context": "instrument1",
                "message": "copy failed",
            },
            {
                "error_class": "TestAlert",
                "context": "instrument2",
                "message": "disk full",
            },
        ]

        with patch("monitoring.alerts.base_alert._suppressions", suppressions):
            # when
            result = alert.get_issues([])

            # then
            assert result == [
                ("instrument3", "network error"),
                ("instrument4", "validation error"),
            ]

    def test_wrong_error_class_not_suppressed(self) -> None:
        """Test that suppressions for different error classes don't affect this alert."""
        # given
        alert = TestAlert()
        alert.test_issues = [("instrument1", "error message")]

        suppressions = [
            {
                "error_class": "DifferentAlert",
                "context": "instrument1",
                "message": "error message",
            }
        ]

        with patch("monitoring.alerts.base_alert._suppressions", suppressions):
            # when
            result = alert.get_issues([])

            # then
            assert result == [("instrument1", "error message")]
