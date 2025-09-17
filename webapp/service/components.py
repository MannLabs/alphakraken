"""UI components for the web application."""

import os
import re
from collections import defaultdict
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Any

import humanize
import matplotlib as mpl
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from matplotlib import pyplot as plt
from service.session_state import SessionStateKeys, copy_session_state
from service.utils import BASELINE_PREFIX, DEFAULT_MAX_AGE_STATUS, display_plotly_chart

from shared.db.models import TERMINAL_STATUSES, KrakenStatusEntities, RawFileStatus
from shared.keys import EnvVars, InstrumentTypes


def _re_filter(text: Any, filter_: str) -> bool:  # noqa: ANN401
    """Filter a value `x` with a `filter_` string."""
    return bool(re.search(filter_, str(text), re.IGNORECASE))


def show_filter(
    df: pd.DataFrame,
    *,
    default_value: str | None = None,
    text_to_display: str = "Filter:",
    example_text: str = "P123",
    st_display: st.delta_generator.DeltaGenerator = st,
) -> tuple[pd.DataFrame, str | None, list[str]]:
    """Filter the DataFrame on user input by case-insensitive textual comparison in all columns.

    :param df: The DataFrame to filter.
    :param text_to_display: The text to display next to the input field.
    :param example_text: An example text to display in the input field and the help text.
    :param st_display: The streamlit display object.

    :return: The filtered DataFrame.
    """
    user_input = st_display.text_input(
        text_to_display,
        st.session_state.get(SessionStateKeys.CURRENT_FILTER, default_value),
        placeholder=f"example: {example_text}",
        help="Case insensitive filter. Chain multiple conditions with `&`, negate with `!`. "
        "Append a column name followed by `=` to filter a specific column, otherwise each column of the table is considered. "
        "When searching a column, range search is done by `column=[lower, upper]`. "
        f"Supports regular expressions. "
        f"Example: `{example_text}`",
        key="current_filter_widget_key",
        on_change=partial(
            copy_session_state,
            SessionStateKeys.CURRENT_FILTER,
            "current_filter_widget_key",
        ),
    )

    mask = [True] * len(df)
    errors = []
    if user_input is not None and user_input != "":
        filters = [f.strip() for f in user_input.lower().split("&")]

        for filter_ in filters:
            negate = False
            column = None
            upper, lower = None, None

            try:
                if filter_.startswith("!"):
                    negate = True
                    filter_ = filter_[1:].strip()  # noqa: PLW2901

                if "=" in filter_:
                    # separate "column=value" -> column, value
                    column, filter_ = filter_.split("=", maxsplit=1)  # noqa: PLW2901

                if (
                    "[" in filter_
                    and "]" in filter_
                    and filter_.index("[") < filter_.index("]")
                ):
                    # extract "[1, 2]" -> 1, 2
                    lower, upper = (
                        filter_.split("[", maxsplit=1)[1]
                        .split("]", maxsplit=1)[0]
                        .split(",")
                    )

                if column is not None:
                    if upper and lower:
                        new_mask = df[column].map(
                            lambda x: float(lower) <= float(x) <= float(upper)
                        )
                    else:
                        new_mask = df[column].map(lambda x: _re_filter(x, filter_))
                else:
                    new_mask = df.map(lambda x: _re_filter(x, filter_)).any(axis=1)
                    new_mask |= df.index.map(lambda x: _re_filter(x, filter_))
            except (re.error, ValueError, KeyError) as e:
                errors.append(
                    f"Could not parse filter {filter_}: ignoring it. {type(e)}: '{e}'"
                )
                continue

            if negate:
                new_mask = ~new_mask

            mask &= new_mask

        # always show baseline data
        mask |= df.index.map(
            lambda x: isinstance(x, str) and x.startswith(BASELINE_PREFIX)
        ).any()

    return df[mask], user_input, errors


def show_date_select(
    df: pd.DataFrame,
    text_to_display: str = "Earliest file creation date:",
    help_to_display: str = "Selects the earliest file creation date to display in table and plots.",
    st_display: st.delta_generator.DeltaGenerator = st,
    max_age_days: int | None = None,
) -> pd.DataFrame:
    """Filter the DataFrame on user input by date."""
    if len(df) == 0:
        return df

    oldest_file = df["created_at"].min()
    youngest_file = df["created_at"].max()
    max_age = (
        datetime.now() - timedelta(days=max_age_days) if max_age_days else oldest_file  # noqa: DTZ005
    )
    last_selectable_date = min(youngest_file, max(oldest_file, max_age))
    min_date = st_display.date_input(
        text_to_display,
        min_value=oldest_file,
        max_value=youngest_file,
        value=last_selectable_date,
        help=help_to_display,
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

    display_plotly_chart(fig, display)


def show_time_in_status_table(
    combined_df: pd.DataFrame,
    display: st.delta_generator.DeltaGenerator,
    ignored_status: list[str] = TERMINAL_STATUSES,
) -> None:
    """Show a table displaying per instrument and status the timestamp of the oldest transition."""
    df = combined_df.copy()
    df["updated_at"] = pd.to_datetime(df["updated_at_"])
    df = df.sort_values("updated_at", ascending=False)

    latest_updates = (
        df.groupby(["instrument_id", "status"])["updated_at"].last().unstack()  # noqa: PD010
    )

    latest_updates = latest_updates.drop(columns=ignored_status, errors="ignore")

    reshaped = latest_updates.sort_index()

    columns = reshaped.columns
    green_ages_m = [1.5 * 60] * len(columns)
    red_ages_m = [3 * 60] * len(
        columns
    )  # both quanting and waiting for new files timeout is 2h
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
    st.write(
        f"Note: for performance reasons, by default only data for the last {DEFAULT_MAX_AGE_STATUS} days are loaded, "
        f"which means that mass specs that have been idling for longer than this period are not shown here."
    )
    status_data = defaultdict(list)

    # Get all entries to display (instruments + filesystems)
    all_entries = []

    # Add instrument entries (with raw files)
    for instrument_id in sorted(combined_df["instrument_id"].unique()):
        tmp_df = combined_df[combined_df["instrument_id"] == instrument_id]
        status_df = status_data_df[status_data_df["_id"] == instrument_id]
        if len(status_df) > 0:
            all_entries.append(("instrument", instrument_id, tmp_df, status_df.iloc[0]))

    # Add filesystem entries (without raw files)
    filesystem_entries = status_data_df[
        status_data_df["entity_type"] == KrakenStatusEntities.FILE_SYSTEM
    ]
    for _, filesystem_row in filesystem_entries.iterrows():
        all_entries.append(("filesystem", filesystem_row["_id"], None, filesystem_row))

    job_entries = status_data_df[
        status_data_df["entity_type"] == KrakenStatusEntities.JOB
    ]
    for _, job_row in job_entries.iterrows():
        all_entries.append(("job", job_row["_id"], None, job_row))

    # Process all entries uniformly
    for entry_type, entry_id, raw_files_df, status_row in all_entries:
        display_name = (
            f"{entry_id} (file system)" if entry_type == "filesystem" else entry_id
        )
        status_data["instrument_id"].append(display_name)

        if entry_type == "instrument" and raw_files_df is not None:
            # timestamp of youngest file
            last_file_creation = raw_files_df.iloc[0]["created_at"]
            status_data["last_file_creation"].append(last_file_creation)
            status_data["last_file_creation_text"].append(
                get_display_time(last_file_creation, now)
            )

            # last status update (e.g. 'quanting' -> 'done')
            last_update = pd.to_datetime(
                sorted(raw_files_df["updated_at_"].to_numpy())[::-1][0]
            )
            status_data["last_status_update"].append(last_update)
            status_data["last_status_update_text"].append(
                get_display_time(last_update, now)
            )
        else:
            # Filesystem entries don't have file creation/update times
            status_data["last_file_creation"].append("-")
            status_data["last_file_creation_text"].append("-")
            status_data["last_status_update"].append("-")
            status_data["last_status_update_text"].append("-")

        # Health check, status, and disk space (common to both types)
        last_health_check = status_row["updated_at_"]
        status_data["last_health_check"].append(last_health_check)
        status_data["last_health_check_text"].append(
            get_display_time(last_health_check, now)
        )

        status_data["status"].append(status_row["status"])
        status_data["status_details"].append(status_row["status_details"])
        status_data["free_space_gb"].append(status_row["free_space_gb"])

    status_df = pd.DataFrame(status_data)

    st.dataframe(
        status_df.style.apply(lambda row: _get_color(row), axis=1)
        # here we misuse highlight_status_cell (for 'error' and 'ok')
        .apply(highlight_status_cell, axis=1)
    )


def get_display_time(
    past_time: datetime, now: datetime, prefix: str = "", suffix: str = " ago"
) -> str:
    """Get a human readable time display.

    :param past_time: The past time to calculate the difference from.
    :param now: The current time to calculate the difference to.
    :param prefix: A string to prepend to the display time. Defaults to an empty string.
    :param suffix: A string to append to the display time. Defaults to " ago".
    :return:
    """
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
    return f"{prefix}{display_time}{suffix}"


def _get_color(
    row: pd.Series,
    columns: list[str] = [  # noqa: B006
        "last_file_creation",
        "last_status_update",
        "last_health_check",
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
        if column not in row or pd.isna(row[column]) or row[column] == "-":
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
    status = row["status"].lower()

    if status == RawFileStatus.ERROR:
        style = "background-color: darkred"
    elif status in [RawFileStatus.QUANTING_FAILED, RawFileStatus.ACQUISITION_FAILED]:
        style = "background-color: red"
    elif status in [RawFileStatus.DONE, RawFileStatus.DONE_NOT_QUANTED]:
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
        should be used for testing only.
        Data can be incomplete, wrong or gone at any time.

        You probably rather want to visit the 'production' environment.
        """,
            icon="⚠️",
        )


def get_terminal_status_counts(
    filtered_df: pd.DataFrame, statuses: list[str] = TERMINAL_STATUSES
) -> str:
    """Count the number of rows and calculate percentages for terminal statuses in the filtered DataFrame.

    Args:
    ----
        filtered_df (pd.DataFrame): The filtered DataFrame containing the 'status' column.
        statuses (list[str]): The statuses to consider.

    Returns:
    -------
        str: A display-ready string with terminal status and 'count' and 'percentage' for each status.

    """
    terminal_df = filtered_df[filtered_df["status"].isin(statuses)]

    if (total_terminal_rows := len(terminal_df)) == 0:
        return "n/a"

    status_counts = terminal_df["status"].value_counts().sort_values()

    result = []
    for status, count in status_counts.items():
        percentage = (count / total_terminal_rows) * 100
        result.append(f"{status}: {int(count)} ({percentage:.1f}%)")

    return "; ".join(result)


def get_full_backup_path(df: pd.DataFrame) -> tuple[list, bool]:  # noqa: C901 (too complex)
    """Construct full path to files by concatenating 'backup_base_path' with the relevant keys from the 'file_info' dictionary.

    Args:
        df (pd.DataFrame): DataFrame containing the columns 'backup_base_path' and 'file_info'.

    Returns:
        paths, is_multiple_types: A list of backup paths and a boolean indicating if multiple instrument types are present.

    """

    def _get_instrument_type(file_info: dict) -> str:
        """Get the instrument type from the file_info dictionary."""
        # TODO: remove this logic! add this info to the raw_file entity -> requires migration

        for key in file_info:
            if key.endswith(".raw"):
                return InstrumentTypes.THERMO
            if key.endswith(".wiff"):
                return InstrumentTypes.SCIEX
            if any(part.endswith(".d") for part in key.split("/")):
                return InstrumentTypes.BRUKER

        raise ValueError(f"Unknown file type in {file_info=}")

    def _get_concatenated_path(raw_file_row: pd.Series) -> tuple[list[str], str]:
        """Get the concatenated path(s) for a raw file.

        - one .raw file for Thermo
        - one .d folder for Brujker
        - multiple .wiff* files for Sciex
        """
        backup_base_path_str = raw_file_row.get("backup_base_path", "")
        file_info = raw_file_row.get("file_info", {})

        if not backup_base_path_str or not file_info:
            raise ValueError(
                f"Missing {backup_base_path_str=} or {file_info=} for file {raw_file_row['_id']}. Please exclude from selection."
            )

        backup_base_path = Path(backup_base_path_str)
        instrument_type = _get_instrument_type(file_info)

        paths = []
        if instrument_type == InstrumentTypes.THERMO:
            first_file_path = next(iter(file_info))
            paths.append(backup_base_path / first_file_path)
        elif instrument_type == InstrumentTypes.SCIEX:
            for file_path in file_info:
                paths.append(backup_base_path / file_path)  # noqa: PERF401
        elif instrument_type == InstrumentTypes.BRUKER:
            paths_ = []
            first_file_path = next(iter(file_info))
            # extract everything up to the .d folder: /path/to/raw_file.d/analysis.tdf -> /path/to/raw_file.d
            for k in first_file_path.split("/"):
                paths_.append(k)
                if k.endswith(".d"):
                    paths.append(backup_base_path / "/".join(paths_))
                    break

        return paths, instrument_type

    result = [
        r for r in df.apply(_get_concatenated_path, axis=1).tolist() if r is not None
    ]

    unique_instrument_types = {sublist[1] for sublist in result}
    return [str(path) for sublist in result for path in sublist[0]], len(
        unique_instrument_types
    ) > 1
