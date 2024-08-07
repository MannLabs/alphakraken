"""UI components for the web application."""

import os
from collections import defaultdict
from datetime import datetime, timedelta

import humanize
import matplotlib as mpl
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from matplotlib import pyplot as plt
from service.utils import TERMINAL_STATUSES

from shared.db.models import RawFileStatus
from shared.keys import EnvVars


# TODO: if filter is set, set age filter to youngest file
def show_filter(
    df: pd.DataFrame,
    *,
    text_to_display: str = "Filter:",
    st_display: st.delta_generator.DeltaGenerator = st,
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
    st_display: st.delta_generator.DeltaGenerator = st,
) -> pd.DataFrame:
    """Filter the DataFrame on user input by date."""
    if len(df) == 0:
        return df
    oldest_file = df["created_at"].min()
    youngest_file = df["created_at"].max()
    two_weeks_ago = datetime.now() - timedelta(days=7 * 2)  # noqa:  DTZ005 no tz argument
    last_selectable_date = min(youngest_file, max(oldest_file, two_weeks_ago))
    min_date = st_display.date_input(
        text_to_display,
        min_value=oldest_file,
        max_value=youngest_file,
        value=last_selectable_date,
    )
    min_date_with_time = datetime.combine(min_date, datetime.min.time())
    return df[df["created_at"] > min_date_with_time]


def show_status_plot(
    combined_df: pd.DataFrame,
    display: st.delta_generator.DeltaGenerator,
    ignored_status: list[str] = TERMINAL_STATUSES,
) -> None:
    """Show a plot of the file statuses for each instrument."""
    status_counts = (
        combined_df.groupby(["instrument_id", "status"]).size().unstack(fill_value=0)  # noqa: PD010
    )

    status_counts.drop(columns=ignored_status, errors="ignore", inplace=True)  # noqa: PD002
    status_counts.sort_index(inplace=True)  # noqa: PD002

    fig = go.Figure()

    for status in status_counts.columns:
        fig.add_trace(
            go.Bar(
                x=status_counts.index,
                y=status_counts[status],
                name=status,
                text=status_counts[status],
                textposition="inside",
            )
        )

    # Update the layout
    fig.update_layout(
        barmode="stack",
        xaxis_title="Instrument",
        yaxis_title="Count",
        legend_title="Status",
        width=500,
        height=500,
    )

    display.plotly_chart(fig)


def show_time_in_status_table(
    combined_df: pd.DataFrame,
    display: st.delta_generator.DeltaGenerator,
    ignored_status: list[str] = TERMINAL_STATUSES,
) -> None:
    """Show a table displaying per instrument and status the timestamp of the oldest transistion."""
    df = combined_df.copy()
    df["updated_at"] = pd.to_datetime(df["updated_at_"])
    df = df.sort_values("updated_at", ascending=False)

    latest_updates = (
        df.groupby(["instrument_id", "status"])["updated_at"].last().unstack()  # noqa: PD010
    )

    latest_updates = latest_updates.drop(columns=ignored_status, errors="ignore")

    reshaped = latest_updates.sort_index()

    columns = reshaped.columns
    green_ages_m = [0.5 * 60] * len(columns)
    red_ages_m = [2 * 60] * len(columns)
    colormaps = ["RdYlGn_r"] * len(columns)
    display.dataframe(
        reshaped.style.apply(
            lambda row: _get_color(
                row,
                columns=columns,
                green_ages_m=green_ages_m,
                red_ages_m=red_ages_m,
                colormaps=colormaps,
            ),
            axis=1,
        )
    )


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

        # timestamp of youngest file
        last_file_creation = tmp_df.iloc[0]["created_at"]
        status_data["last_file_creation"].append(last_file_creation)
        status_data["last_file_creation_text"].append(
            _get_display_time(last_file_creation, now)
        )

        # last status update (e.g. 'quanting' -> 'done')
        last_update = sorted(tmp_df["updated_at_"].to_numpy())[::-1][0]
        status_data["last_status_update"].append(last_update)
        status_data["last_status_update_text"].append(
            _get_display_time(last_update, now)
        )

        # last file watcher poke
        last_file_check = status_df["updated_at_"].to_numpy()[0]
        status_data["last_file_check"].append(last_file_check)
        status_data["last_file_check_text"].append(
            _get_display_time(last_file_check, now)
        )
        status_data["last_file_check_error"].append(
            status_df["last_error_occurred_at"].to_numpy()[0]
        )
        status_data["status_details"].append(status_df["status_details"].to_numpy()[0])

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
    green_ages_m: list[float] = [  # noqa: B006
        2 * 60,
        2 * 60,
        5.1,  # should be larger than HEALTH_CHECK_INTERVAL_M
    ],
    red_ages_m: list[float] = [  # noqa: B006
        8 * 60,
        8 * 60,
        10,  # could take up to 5 minutes to resume checking after worker restart
    ],
    colormaps: list[str] = [  # noqa: B006
        "summer",
        "summer",
        "RdYlGn_r",
    ],
) -> list[str | None]:
    """Get the color for the row based on the age of the columns.

    :param row: a row of the status dataframe
    :param columns: which columns to color
    :param green_ages_m: for each column: everything younger than this is green (in minutes)
    :param red_ages_m:  for each column: everything older than this is red (in minutes)
    :return: style for the row, e.g. [ "background-color: #FF0000", None, None]
    """
    column_styles = {}
    now = datetime.now()  # noqa:  DTZ005 no tz argument
    for column, green_age_m, red_age_m, colormap in zip(
        columns, green_ages_m, red_ages_m, colormaps, strict=True
    ):
        if column not in row or pd.isna(row[column]):
            continue

        time_delta = now - row[column]

        normalized_age = (
            time_delta - timedelta(minutes=green_age_m)
        ).total_seconds() / ((red_age_m - green_age_m) * 60)
        normalized_age = min(1.0, max(0.0, normalized_age))

        color = plt.get_cmap(colormap)(normalized_age)

        style = "background-color: " + mpl.colors.rgb2hex(color)
        column_styles[column] = style

    return [column_styles.get(c) for c in row.index]


def highlight_status_cell(row: pd.Series) -> list[str | None]:
    """Highlight a single cell based on its value."""
    status = row["status"]

    if status == RawFileStatus.ERROR:
        style = "background-color: darkred"
    elif status in [RawFileStatus.QUANTING_FAILED, RawFileStatus.ACQUISITION_FAILED]:
        style = "background-color: red"
    elif status == RawFileStatus.DONE:
        style = "background-color: green"
    elif status == RawFileStatus.IGNORED:
        style = "background-color: lightgray"
    else:
        style = "background-color: #aed989"

    column_styles = {"status": style}
    return [column_styles.get(c) for c in row.index]


def show_sandbox_message() -> None:
    """Show a warning message if the environment is sandbox."""
    if os.environ.get(EnvVars.ENV_NAME) == "sandbox":
        st.error(
            """
        Note: you are currently viewing the 'sandbox' environment which
        should be used for testing by AlphaKraken admins only. Everything could
        change any minute, and data could be wrong or gone at any time.

        You probably rather want to visit the 'production' environment:
        [http://<kraken_url>](http://<kraken_url>).
        """,
            icon="⚠️",
        )
