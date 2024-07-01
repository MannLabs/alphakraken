"""UI components for the web application."""

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

import humanize
import matplotlib as mpl
import pandas as pd
import streamlit as st
from matplotlib import pyplot as plt


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
        placeholder="e.g. test2 & !hela",
        help="Case insensitive. Chain multiple conditions with '&', negate with '!'. E.g. test2 & qc & !hela.",
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


def display_status(combined_df: pd.DataFrame, status_data_df: pd.DataFrame) -> None:
    """Display the status of the kraken."""
    now = datetime.now()  # noqa:  DTZ005 no tz argument
    st.write(
        f"Current Kraken time: {now.replace(microsecond=0)} [all time stamps are given in UTC!]"
    )
    status_data = defaultdict(list)
    for instrument_id in sorted(combined_df["instrument_id"].unique()):
        tmp_df = combined_df[combined_df["instrument_id"] == instrument_id]
        status_df = status_data_df[status_data_df["_id"] == instrument_id]

        status_data["instrument_id"].append(instrument_id)

        last_file_check = status_df["updated_at_"].to_numpy()[0]
        status_data["last_file_check"].append(last_file_check)
        status_data["last_file_check_text"].append(
            _get_display_time(last_file_check, now)
        )
        status_data["last_file_check_error"].append(
            status_df["last_error_occurred_at"].to_numpy()[0]
        )

        last_file_creation = tmp_df.iloc[0]["created_at"]
        status_data["last_file_creation"].append(last_file_creation)
        status_data["last_file_creation_text"].append(
            _get_display_time(last_file_creation, now)
        )

        last_update = sorted(tmp_df["updated_at_"].to_numpy())[::-1][0]
        status_data["last_status_update"].append(last_update)
        status_data["last_status_update_text"].append(
            _get_display_time(last_update, now)
        )

    status_df = pd.DataFrame(status_data)

    st.dataframe(status_df.style.apply(lambda row: _get_color(row), axis=1))


def _get_display_time(past_time: datetime, now: datetime) -> str:
    """Get a human readable display time for the last file creation."""
    display_time = humanize.precisedelta(
        now - pd.Timestamp(past_time), minimum_unit="seconds", format="%.0f"
    )
    for full, abbrev in {
        " seconds": "s",
        " second": "s",
        " minutes": "m",
        " minute": "m",
        " hours": "h",
        " hour": "h",
    }.items():
        display_time = display_time.replace(full, abbrev)
    return f"{display_time} ago"


def _get_color(
    row: pd.Series,
    columns: list[str] = [  # noqa: B006
        "last_file_creation",
        "last_status_update",
        "last_file_check",
    ],
    green_ages_h: list[float] = [  # noqa: B006
        2,
        2,
        0.02,
    ],
    red_ages_h: list[float] = [  # noqa: B006
        8,
        8,
        0.1,  # could take up to 5 minutes to resume checking after worker restart
    ],
) -> list[str | None]:
    """Get the color for the row based on the age of the columns.

    :param row: a row of the status dataframe
    :param columns: which columns to color
    :param green_ages_h: for each column: everything younger than this is green
    :param red_ages_h:  for each column: everything older than this is red
    :return: style for the row, e.g. [ "background-color: #FF0000", None, None]
    """
    column_styles = {}
    now = datetime.now()  # noqa:  DTZ005 no tz argument
    for column, green_age_h, red_age_h in zip(
        columns, green_ages_h, red_ages_h, strict=True
    ):
        time_delta = now - row[column]

        normalized_age = (time_delta - timedelta(hours=green_age_h)).total_seconds() / (
            (red_age_h - green_age_h) * 3600
        )
        normalized_age = min(1.0, max(0.0, normalized_age))

        color = plt.get_cmap("RdYlGn_r")(
            normalized_age
        )  # Inverse of the Red-Yellow-Green colormap

        style = "background-color: " + mpl.colors.rgb2hex(color)
        column_styles[column] = style

    return [column_styles.get(c) for c in row.index]


def highlight_status_cell(row: pd.Series) -> list[str | None]:
    """Highlight a single cell based on its value."""
    status = row["status"]

    if status == "error":
        style = "background-color: darkred"
    elif status == "done":
        style = "background-color: green"
    elif status == "ignored":
        style = "background-color: lightgray"
    else:
        style = "background-color: #aed989"

    column_styles = {"status": style}
    return [column_styles.get(c) for c in row.index]
