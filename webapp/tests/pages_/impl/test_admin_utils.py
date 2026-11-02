"""Tests for the admin page logic."""

from datetime import date, datetime, timedelta

import pytest
import pytz
from pages_.impl.admin_utils import (
    end_of_day,
    get_suppressions_df,
    validate_suppression,
)
from service.timezone import DISPLAY_TIMEZONE

from shared.db.models import AlertSuppression

VALID = {
    "alert": "PumpPressureAlert",
    "instrument_id": "*",
    "identifier": "*",
    "details": "*",
    "comment": "pump under maintenance",
}


def test_validate_suppression_should_accept_restricted_suppression() -> None:
    """A suppression restricting at least one field with a comment is valid."""
    # when / then
    validate_suppression(**VALID)


@pytest.mark.parametrize(
    ("overrides", "match"),
    [
        ({"alert": " "}, "Alert is required"),
        ({"instrument_id": ""}, "Instrument is required"),
        ({"identifier": " "}, "Identifier is required"),
        ({"details": ""}, "Details is required"),
        ({"comment": " "}, "Comment is required"),
        ({"alert": "*"}, "would mute all alerts"),
    ],
)
def test_validate_suppression_should_reject_invalid_suppression(
    overrides: dict[str, str], match: str
) -> None:
    """Suppressions with an empty field or matching everything are rejected."""
    # when / then
    with pytest.raises(ValueError, match=match):
        validate_suppression(**{**VALID, **overrides})


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
        AlertSuppression(comment="old", expires_at=now - timedelta(days=1)),
        AlertSuppression(comment="future", expires_at=now + timedelta(days=1)),
        AlertSuppression(comment="never"),
    ]

    # when
    df = get_suppressions_df(suppressions)

    # then
    assert df["comment"].tolist() == ["old", "future", "never"]
    assert df["expired"].tolist() == [True, False, False]
    assert df["alert"].tolist() == ["*", "*", "*"]
