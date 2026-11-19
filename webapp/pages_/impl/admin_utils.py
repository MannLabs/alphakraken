"""Logic of the admin page."""

from datetime import date, datetime, time

import pandas as pd
from service.timezone import DISPLAY_TIMEZONE, display_now

from shared.domain.alerts import AlertSuppression


def end_of_day(day: date) -> datetime:
    """Return the last moment of the given day in the display timezone."""
    return DISPLAY_TIMEZONE.localize(datetime.combine(day, time.max))


def get_suppressions_df(suppressions: list[AlertSuppression]) -> pd.DataFrame:
    """Get a DataFrame for displaying the suppressions, including whether they are expired."""
    df = pd.DataFrame(
        [
            {
                "alert": s.alert,
                "instrument": s.instrument_id,
                "identifier": s.identifier,
                "details": s.details,
                "comment": s.comment,
                "expires at": s.expires_at,
                "created by": s.created_by,
                "created at": s.created_at_,
            }
            for s in suppressions
        ]
    )
    # all timestamps in the database are UTC
    for column in ("expires at", "created at"):
        df[column] = pd.to_datetime(df[column], utc=True)
    df.insert(
        df.columns.get_loc("expires at") + 1,
        "expired",
        df["expires at"] < display_now(),
    )
    return df
