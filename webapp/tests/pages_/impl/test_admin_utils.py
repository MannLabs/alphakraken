"""Tests for the admin page logic."""

from datetime import date, datetime, timedelta

import pytz
from pages_.impl.admin_utils import end_of_day, get_suppressions_df
from service.timezone import DISPLAY_TIMEZONE

from shared.tests.factories import alert_suppression


def test_end_of_day_should_return_last_moment_of_day_in_display_timezone() -> None:
    """The expiry date is inclusive, in the display timezone."""
    # when
    result = end_of_day(date(2026, 3, 1))

    # then
    assert result.tzinfo is not None
    assert result.astimezone(DISPLAY_TIMEZONE).date() == date(2026, 3, 1)
    assert (result + timedelta(microseconds=1)).astimezone(
        DISPLAY_TIMEZONE
    ).date() == date(2026, 3, 2)


def test_get_suppressions_df_should_mark_expired_suppressions() -> None:
    """Expiry is evaluated against the current time; suppressions without expiry never expire."""
    # given
    now = datetime.now(tz=pytz.UTC)
    suppressions = [
        alert_suppression(comment="old", expires_at=now - timedelta(days=1)),
        alert_suppression(comment="future", expires_at=now + timedelta(days=1)),
        alert_suppression(comment="never"),
    ]

    # when
    df = get_suppressions_df(suppressions)

    # then
    assert df["comment"].tolist() == ["old", "future", "never"]
    assert df["expired"].tolist() == [True, False, False]
    assert df["alert"].tolist() == ["*", "*", "*"]
