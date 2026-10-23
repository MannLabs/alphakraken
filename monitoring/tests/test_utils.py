"""Unit tests for monitoring utils."""

from datetime import datetime

import pytz

from monitoring.utils import ensure_utc


def test_ensure_utc_should_treat_naive_datetime_as_utc() -> None:
    """A naive datetime is interpreted as UTC."""
    # when / then
    naive = datetime(2026, 1, 1, 12, 0)  # noqa: DTZ001
    assert ensure_utc(naive) == datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)


def test_ensure_utc_should_keep_aware_datetime() -> None:
    """An aware datetime is returned unchanged."""
    # given
    value = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
    # when / then
    assert ensure_utc(value) is value
