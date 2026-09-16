"""Access to the timezone that the webapp displays timestamps in."""

from datetime import datetime
from typing import cast

import pandas as pd

from shared.yamlsettings import get_timezone

DISPLAY_TIMEZONE = get_timezone()
DISPLAY_TIMEZONE_NAME = str(DISPLAY_TIMEZONE)


def display_now() -> datetime:
    """Return the current wall-clock time in the display timezone, without tzinfo."""
    return datetime.now(tz=DISPLAY_TIMEZONE).replace(tzinfo=None)


def to_display_timezone(df: pd.DataFrame) -> pd.DataFrame:
    """Shift every timestamp column of a database-derived DataFrame to the display timezone.

    All timestamps in the database are UTC, so they are identified by their dtype rather than
    by name: this also covers the dynamic fields of the Metrics documents. Durations and dates
    have dtypes of their own and are left alone.

    The shifted columns carry no tzinfo, so that every comparison, filter and plot downstream
    operates on wall-clock values of one single timezone.
    """
    for column in df.select_dtypes(include=["datetime", "datetimetz"]).columns:
        timestamps = cast("pd.Series", pd.to_datetime(df[column], utc=True))
        df[column] = timestamps.dt.tz_convert(DISPLAY_TIMEZONE).dt.tz_localize(None)
    return df
