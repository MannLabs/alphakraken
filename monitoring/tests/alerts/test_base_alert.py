"""Unit tests for BaseAlert and Issue."""

from datetime import datetime
from unittest.mock import patch

import pytz

from monitoring.alerts.base_alert import BaseAlert, Issue, _shorten

NOW = datetime(2024, 1, 15, 12, 0, tzinfo=pytz.UTC)


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


def test_get_issues_returns_the_issues_of_the_alert() -> None:
    """get_issues wraps _get_issues."""
    # given / when
    with patch.object(DefaultFormatAlert, "_get_issues", return_value=[Issue("a", 1)]):
        result = DefaultFormatAlert().get_issues([], NOW)

    # then
    assert result == [Issue("a", 1)]


def test_shorten_cuts_long_text_with_an_ellipsis() -> None:
    """Texts above the limit end with an ellipsis within the limit."""
    assert _shorten("abc", 3) == "abc"
    assert _shorten("abcd", 3) == "ab…"


def test_format_message_default_shortens_long_details() -> None:
    """Long details are cut, so that messages stay within the webhook limits."""
    # when
    result = DefaultFormatAlert().format_message([Issue("a", "x" * 400)])

    # then
    assert len(result) < 320
    assert result.endswith("…")
