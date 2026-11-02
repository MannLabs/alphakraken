"""Unit tests for BaseAlert suppression functionality."""

from datetime import datetime
from unittest.mock import patch

import pytz

from monitoring.alerts.base_alert import BaseAlert, Issue, shorten

NOW = datetime(2024, 1, 15, 12, 0, tzinfo=pytz.UTC)


class TestAlert(BaseAlert):
    """Test implementation of BaseAlert for testing purposes."""

    def __init__(self) -> None:
        """Initialize the TestAlert with no issues."""
        super().__init__()
        self.test_issues: list[Issue] = []

    def _get_issues(
        self,
        status_objects: list,  # noqa: ARG002
        now: datetime,  # noqa: ARG002
    ) -> list[Issue]:
        """Mock implementation to return predefined issues."""
        # Return predefined issues for testing
        return self.test_issues if hasattr(self, "test_issues") else []

    def format_message(self, issues: list[Issue]) -> str:
        """Format the alert message for the issues found."""
        return f"Test message with {len(issues)} issues"


class DefaultFormatAlert(BaseAlert):
    """Alert relying on the default `format_message`."""

    title = "Default title"

    def _get_issues(self, status_objects: list, now: datetime) -> list[Issue]:  # noqa: ARG002
        """Not used."""
        return []


def test_issue_key_defaults_to_identifier() -> None:
    """Without an explicit key, the identifier is the key."""
    assert Issue("inst1", 1).key == "inst1"
    assert Issue("inst1", 1, key="inst1:file").key == "inst1:file"


def test_cooldown_defaults_to_the_class_attribute() -> None:
    """Alerts report their class cooldown for every issue."""
    assert DefaultFormatAlert().get_cooldown_minutes(Issue("a", 1)) == 120


def test_format_message_default_lists_one_line_per_issue() -> None:
    """The default message body has one line per issue, without the title."""
    # given
    issues = [Issue("instrument1", "disk full"), Issue("instrument2", 42)]

    # when
    result = DefaultFormatAlert().format_message(issues)

    # then
    assert result == "- `instrument1`: disk full\n- `instrument2`: 42"


class TestBaseAlertSuppressions:
    """Test suite for BaseAlert suppression functionality."""

    def test_no_suppressions_file_returns_all_issues(self) -> None:
        """Test that when no suppressions file exists, all issues are returned."""
        # given
        alert = TestAlert()
        alert.test_issues = [
            Issue("instrument1", "error message"),
            Issue("instrument2", "another error"),
        ]

        with patch("monitoring.alerts.base_alert._suppressions", []):
            # when
            result = alert.get_issues([], NOW)

            # then
            assert result == [
                Issue("instrument1", "error message"),
                Issue("instrument2", "another error"),
            ]

    def test_exact_context_and_message_suppression(self) -> None:
        """Test exact context and message matching suppression."""
        # given
        alert = TestAlert()
        alert.test_issues = [
            Issue("instrument1", "copy failed"),
            Issue("instrument2", "disk full"),
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
            result = alert.get_issues([], NOW)

            # then
            assert result == [Issue("instrument2", "disk full")]

    def test_wildcard_context_suppresses_all_contexts(self) -> None:
        """Test that context: '*' suppresses all contexts for the error class."""
        # given
        alert = TestAlert()
        alert.test_issues = [
            Issue("instrument1", "disk full"),
            Issue("instrument2", "disk full"),
            Issue("backup_system", "disk full"),
        ]

        suppressions = [
            {"error_class": "TestAlert", "context": "*", "message": "disk full"}
        ]

        with patch("monitoring.alerts.base_alert._suppressions", suppressions):
            # when
            result = alert.get_issues([], NOW)

            # then
            assert result == []

    def test_multiple_suppression_rules(self) -> None:
        """Test that multiple suppression rules are all applied."""
        # given
        alert = TestAlert()
        alert.test_issues = [
            Issue("instrument1", "copy failed"),
            Issue("instrument2", "disk full"),
            Issue("instrument3", "network error"),
            Issue("instrument4", "validation error"),
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
            result = alert.get_issues([], NOW)

            # then
            assert result == [
                Issue("instrument3", "network error"),
                Issue("instrument4", "validation error"),
            ]

    def test_wrong_error_class_not_suppressed(self) -> None:
        """Test that suppressions for different error classes don't affect this alert."""
        # given
        alert = TestAlert()
        alert.test_issues = [Issue("instrument1", "error message")]

        suppressions = [
            {
                "error_class": "DifferentAlert",
                "context": "instrument1",
                "message": "error message",
            }
        ]

        with patch("monitoring.alerts.base_alert._suppressions", suppressions):
            # when
            result = alert.get_issues([], NOW)

            # then
            assert result == [Issue("instrument1", "error message")]


def test_shorten_cuts_long_text_with_an_ellipsis() -> None:
    """Texts above the limit end with an ellipsis within the limit."""
    assert shorten("abc", 3) == "abc"
    assert shorten("abcd", 3) == "ab…"


def test_format_message_default_shortens_long_details() -> None:
    """Long details are cut, so that messages stay within the webhook limits."""
    # when
    result = DefaultFormatAlert().format_message([Issue("a", "x" * 400)])

    # then
    assert len(result) < 320
    assert result.endswith("…")
