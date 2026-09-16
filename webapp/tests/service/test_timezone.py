"""Tests for the timezone module."""

from collections.abc import Generator
from datetime import datetime, timedelta
from unittest.mock import patch

import pandas as pd
import pytest
import pytz
from service.timezone import display_now, to_display_timezone

BERLIN = pytz.timezone("Europe/Berlin")


@pytest.fixture
def _berlin_timezone() -> Generator[None, None, None]:
    """Set the display timezone to Europe/Berlin."""
    with patch("service.timezone.DISPLAY_TIMEZONE", BERLIN):
        yield


@pytest.mark.usefixtures("_berlin_timezone")
def test_display_now_returns_naive_time_in_display_timezone() -> None:
    """Test that display_now returns the wall-clock time of the display timezone."""
    # when
    now = display_now()

    assert now.tzinfo is None
    assert now - datetime.now(tz=BERLIN).replace(tzinfo=None) < pd.Timedelta(seconds=1)


@pytest.mark.usefixtures("_berlin_timezone")
def test_to_display_timezone_shifts_naive_utc_timestamps() -> None:
    """Test that naive UTC timestamps are shifted to the display timezone."""
    df = pd.DataFrame(
        {
            "created_at": [datetime(2024, 1, 1, 12, 0, 0)],  # noqa: DTZ001
            "created_at_": [datetime(2024, 7, 1, 12, 0, 0)],  # noqa: DTZ001
            "updated_at_": [datetime(2024, 7, 1, 12, 0, 0)],  # noqa: DTZ001
        }
    )

    # when
    result = to_display_timezone(df)

    assert result["created_at"].tolist() == [pd.Timestamp("2024-01-01 13:00:00")]  # CET
    assert result["created_at_"].tolist() == [
        pd.Timestamp("2024-07-01 14:00:00")
    ]  # CEST
    assert result["updated_at_"].tolist() == [pd.Timestamp("2024-07-01 14:00:00")]


@pytest.mark.usefixtures("_berlin_timezone")
def test_to_display_timezone_shifts_aware_utc_timestamps() -> None:
    """Test that timezone-aware UTC timestamps are shifted to the display timezone."""
    df = pd.DataFrame({"created_at": [datetime(2024, 7, 1, 12, 0, 0, tzinfo=pytz.UTC)]})

    # when
    result = to_display_timezone(df)

    assert result["created_at"].tolist() == [pd.Timestamp("2024-07-01 14:00:00")]


@pytest.mark.usefixtures("_berlin_timezone")
def test_to_display_timezone_ignores_non_timestamp_columns() -> None:
    """Test that a column that does not hold timestamps is left untouched, despite its name."""
    df = pd.DataFrame({"created_at": [1], "instrument_id": ["test1"]})

    # when
    result = to_display_timezone(df)

    assert result["created_at"].tolist() == [1]
    assert result["instrument_id"].tolist() == ["test1"]


def test_to_display_timezone_keeps_utc_timestamps_unchanged() -> None:
    """Test that the default display timezone UTC leaves timestamps as they are."""
    df = pd.DataFrame({"created_at": [datetime(2024, 7, 1, 12, 0, 0)]})  # noqa: DTZ001

    # when
    result = to_display_timezone(df)

    assert result["created_at"].tolist() == [pd.Timestamp("2024-07-01 12:00:00")]


@pytest.mark.usefixtures("_berlin_timezone")
def test_to_display_timezone_shifts_columns_of_any_name() -> None:
    """Test that timestamp columns are recognized by dtype, not by name."""
    df = pd.DataFrame({"some_dynamic_metric": [datetime(2024, 7, 1, 12, 0, 0)]})  # noqa: DTZ001

    # when
    result = to_display_timezone(df)

    assert result["some_dynamic_metric"].tolist() == [
        pd.Timestamp("2024-07-01 14:00:00")
    ]


@pytest.mark.usefixtures("_berlin_timezone")
def test_to_display_timezone_ignores_durations_and_dates() -> None:
    """Test that durations and dates are left alone."""
    df = pd.DataFrame(
        {
            "duration": [timedelta(seconds=5)],
            "date": [datetime(2024, 7, 1).date()],  # noqa: DTZ001
        }
    )

    # when
    result = to_display_timezone(df)

    assert result["duration"].tolist() == [pd.Timedelta(seconds=5)]
    assert result["date"].tolist() == [datetime(2024, 7, 1).date()]  # noqa: DTZ001
