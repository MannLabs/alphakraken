"""Display and selection of the optional owner of projects and settings."""

from typing import Any

import pandas as pd
import streamlit.delta_generator

from shared.db.interface import get_all_users

NO_OWNER_LABEL = "(none)"
OWNER_COLUMN = "owner"


def add_owner_initials(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of `df` with the owner emails replaced by the owners' initials."""
    if OWNER_COLUMN not in df.columns:
        return df

    initials = {u.email: u.initials for u in get_all_users(include_inactive=True)}
    return df.assign(
        **{OWNER_COLUMN: df[OWNER_COLUMN].map(lambda email: initials.get(email, ""))}
    )


def show_owner_selectbox(
    st_display: streamlit.delta_generator.DeltaGenerator | Any,
    default_owner_email: str | None = None,
) -> str | None:
    """Show a selectbox for the optional owner and return the selected user's email.

    Nothing is shown if there are no active users.
    """
    users = get_all_users()
    if not users:
        return None

    labels = {u.email: f"{u.initials} ({u.email})" for u in users}
    options = [None, *labels]
    return st_display.selectbox(
        label="Owner",
        options=options,
        index=options.index(default_owner_email)
        if default_owner_email in options
        else 0,
        format_func=lambda email: labels.get(email, NO_OWNER_LABEL),
        help="Optional, cannot be changed later.",
    )
