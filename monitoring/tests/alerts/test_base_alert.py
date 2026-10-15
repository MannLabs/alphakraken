"""Unit tests for BaseAlert suppression functionality."""

from unittest.mock import Mock, patch

import pytest

from monitoring.alerts.base_alert import BaseAlert, Issue, get_instrument_id
from shared.db.models import KrakenStatusEntities


class TestAlert(BaseAlert):
    """Test implementation of BaseAlert for testing purposes."""

    def __init__(self) -> None:
        """Initialize the TestAlert with no issues."""
        super().__init__()
        self.test_issues: list[Issue] = []

    @property
    def name(self) -> str:
        """Return the name of the alert."""
        return "test_alert"

    def _get_issues(
        self,
        status_objects: list,  # noqa: ARG002
    ) -> list[Issue]:
        """Mock implementation to return predefined issues."""
        # Return predefined issues for testing
        return self.test_issues if hasattr(self, "test_issues") else []

    def format_message(self, issues: list[Issue]) -> str:
        """Format the alert message for the issues found."""
        return f"Test message with {len(issues)} issues"


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
            result = alert.get_issues([])

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
            result = alert.get_issues([])

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
            result = alert.get_issues([])

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
            result = alert.get_issues([])

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
            result = alert.get_issues([])

            # then
            assert result == [Issue("instrument1", "error message")]

    def test_wildcard_error_class_suppresses_all_alerts_for_context(self) -> None:
        """Test that error_class: '*' suppresses matching contexts of any alert class."""
        # given
        alert = TestAlert()
        alert.test_issues = [
            Issue("instrument1", "disk full"),
            Issue("absolute_instrument1", "high pressure"),
            Issue("instrument2", "disk full"),
        ]

        suppressions = [{"error_class": "*", "context": "instrument1"}]

        with patch("monitoring.alerts.base_alert._suppressions", suppressions):
            # when
            result = alert.get_issues([])

            # then
            assert result == [Issue("instrument2", "disk full")]

    def test_missing_error_class_not_suppressed(self) -> None:
        """Test that a rule without error_class matches no alert class."""
        # given
        alert = TestAlert()
        alert.test_issues = [Issue("instrument1", "disk full")]

        suppressions = [{"context": "instrument1"}]

        with patch("monitoring.alerts.base_alert._suppressions", suppressions):
            # when
            result = alert.get_issues([])

            # then
            assert result == [Issue("instrument1", "disk full")]

    def test_instrument_id_suppression_matches_exactly(self) -> None:
        """Test that instrument_id suppresses only issues of exactly that instrument."""
        # given
        alert = TestAlert()
        alert.test_issues = [
            Issue("file_a.raw", "error", instrument_id="astral1"),
            Issue("file_b.raw", "error", instrument_id="astral10"),
            Issue("webapp", "error"),
        ]

        suppressions = [{"error_class": "*", "instrument_id": "astral1"}]

        with patch("monitoring.alerts.base_alert._suppressions", suppressions):
            # when
            result = alert.get_issues([])

            # then
            assert result == [
                Issue("file_b.raw", "error", instrument_id="astral10"),
                Issue("webapp", "error"),
            ]

    def test_instrument_id_suppression_combines_with_message(self) -> None:
        """Test that instrument_id and message must both match."""
        # given
        alert = TestAlert()
        alert.test_issues = [
            Issue("astral1", "disk full", instrument_id="astral1"),
            Issue("astral1", "copy failed", instrument_id="astral1"),
        ]

        suppressions = [
            {"error_class": "TestAlert", "instrument_id": "astral1", "message": "disk"}
        ]

        with patch("monitoring.alerts.base_alert._suppressions", suppressions):
            # when
            result = alert.get_issues([])

            # then
            assert result == [Issue("astral1", "copy failed", instrument_id="astral1")]


@pytest.mark.parametrize(
    ("entity_type", "expected"),
    [
        (KrakenStatusEntities.INSTRUMENT, "astral1"),
        (KrakenStatusEntities.FILE_SYSTEM, None),
        (KrakenStatusEntities.JOB, None),
    ],
)
def test_get_instrument_id_only_for_instruments(
    entity_type: str, expected: str | None
) -> None:
    """Only status objects of instruments yield an instrument id."""
    # given
    kraken_status = Mock(id="astral1", entity_type=entity_type)
    # when / then
    assert get_instrument_id(kraken_status) == expected
