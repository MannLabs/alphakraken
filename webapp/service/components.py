"""UI components for the web application."""

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

import humanize
import pandas as pd
import streamlit as st


def show_filter(
    df: pd.DataFrame,
    *,
    text_to_display: str = "Filter:",
    st_display: Any = st,  # noqa: ANN401
) -> pd.DataFrame:
    """Filter the DataFrame on user input by case-insensitive textual comparison in all columns."""
    user_input = st_display.text_input(
        text_to_display,
        None,
        placeholder="e.g. 'test2 & !hela'",
        help="Chain multiple conditions with '&', negate with '!'",
    )
    if user_input is not None and user_input != "":
        filters = [f.strip() for f in user_input.lower().split("&")]
        mask = [True] * len(df)
        for filter_ in filters:
            negate = False
            if filter_.startswith("!"):
                negate = True
                filter_ = filter_[1:].strip()  # noqa: PLW2901

            new_mask = df.map(lambda x: filter_ in str(x).lower()).any(axis=1)
            new_mask |= df.index.map(lambda x: filter_ in str(x).lower())
            if negate:
                new_mask = ~new_mask

            mask &= new_mask
        return df[mask]
    return df


def show_date_select(
    df: pd.DataFrame,
    text_to_display: str = "Earliest file creation date:",
    st_display: Any = st,  # noqa: ANN401
) -> pd.DataFrame:
    """Filter the DataFrame on user input by date."""
    if len(df) == 0:
        return df
    oldest_file = df["created_at"].min()
    youngest_file = df["created_at"].max()
    two_weeks_ago = datetime.now() - timedelta(days=7 * 2)  # noqa:  DTZ005 no tz argument
    last_selectable_date = max(oldest_file, two_weeks_ago)
    min_date = st_display.date_input(
        text_to_display,
        min_value=oldest_file,
        max_value=youngest_file,
        value=last_selectable_date,
    )
    min_date_with_time = datetime.combine(min_date, datetime.min.time())
    return df[df["created_at"] > min_date_with_time]


def display_status(df: pd.DataFrame) -> None:
    """Display the status of the kraken."""
    now = datetime.now()  # noqa:  DTZ005 no tz argument
    st.write(f"Current Kraken time: {now}")
    status_data = defaultdict(list)
    for instrument_id in df["instrument_id"].unique():
        tmp_df = df[df["instrument_id"] == instrument_id]
        status_data["instrument_id"].append(instrument_id)

        last_file_creation = tmp_df.iloc[0]["created_at"]
        display_time = humanize.precisedelta(
            now - last_file_creation, minimum_unit="seconds", format="%.0f"
        )
        status_data["last_file_creation"].append(last_file_creation)
        status_data["last_file_creation_text"].append(display_time)

        last_update = tmp_df.sort_values(by="updated_at_", ascending=False).iloc[0][
            "updated_at_"
        ]
        display_time = humanize.precisedelta(
            now - last_update, minimum_unit="seconds", format="%.0f"
        )
        status_data["last_status_update"].append(last_update)
        status_data["last_status_update_text"].append(display_time)

    status_df = pd.DataFrame(status_data)
    st.dataframe(status_df)
