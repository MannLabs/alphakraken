"""Access to the timezone that the webapp displays timestamps in."""

from datetime import datetime
from typing import cast

import pandas as pd

from shared.yamlsettings import get_timezone

DISPLAY_TIMEZONE = get_timezone()
DISPLAY_TIMEZONE_NAME = str(DISPLAY_TIMEZONE)


def display_now() -> datetime:
    """Return the current time in the display timezone."""
    return datetime.now(tz=DISPLAY_TIMEZONE)


def to_display_timezone(df: pd.DataFrame) -> pd.DataFrame:
    """Shift every timestamp column of a database-derived DataFrame to the display timezone.

    All timestamps in the database are UTC, so they are identified by their dtype rather than
    by name: this also covers the dynamic fields of the Metrics documents. Durations and dates
    have dtypes of their own and are left alone.

    The shifted columns stay timezone-aware: naive wall-clock values would break duration
    arithmetic across DST transitions.
    """
    for column in df.select_dtypes(include=["datetime", "datetimetz"]).columns:
        timestamps = cast("pd.Series", pd.to_datetime(df[column], utc=True))
        df[column] = timestamps.dt.tz_convert(DISPLAY_TIMEZONE)
    return df


def floor_to_seconds(timestamps: pd.Series) -> pd.Series:
    """Floor timezone-aware timestamps to full seconds."""
    # flooring in a DST-observing timezone raises on ambiguous wall-clock times, so floor in UTC
    return timestamps.dt.tz_convert("UTC").dt.floor("s").dt.tz_convert(DISPLAY_TIMEZONE)
