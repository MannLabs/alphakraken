"""Logic of the admin page."""

from datetime import date, datetime, time

import pandas as pd
from service.timezone import DISPLAY_TIMEZONE, display_now

from shared.db.models import AlertSuppression
from shared.keys import ALERT_SUPPRESSION_WILDCARD


def validate_suppression(
    *, error_class: str, instrument_id: str, context: str, message: str, comment: str
) -> None:
    """Raise ValueError if the suppression is incomplete or would mute every alert."""
    for name, value in (
        ("Alert class", error_class),
        ("Instrument", instrument_id),
        ("Context", context),
        ("Message", message),
    ):
        if not value.strip():
            raise ValueError(f"{name} is required, use '*' for any.")
    if not comment.strip():
        raise ValueError("Comment is required: state why the alerts are muted.")
    if all(
        field.strip() == ALERT_SUPPRESSION_WILDCARD
        for field in (error_class, instrument_id, context, message)
    ):
        raise ValueError("This would mute all alerts. Restrict at least one field.")


def end_of_day(day: date) -> datetime:
    """Return the last moment of the given day in the display timezone."""
    return DISPLAY_TIMEZONE.localize(datetime.combine(day, time.max))


def get_suppressions_df(suppressions: list[AlertSuppression]) -> pd.DataFrame:
    """Get a DataFrame for displaying the suppressions, including whether they are expired."""
    df = pd.DataFrame(
        [
            {
                "alert class": s.error_class,
                "instrument": s.instrument_id,
                "context": s.context,
                "message": s.message,
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
