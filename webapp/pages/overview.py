"""Simple data overview."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.express as px
import pytz

# ruff: noqa: PD002 # `inplace=True` should be avoided; it has inconsistent behavior
import streamlit as st
import yaml
from matplotlib import pyplot as plt
from service.components import (
    get_display_time,
    get_full_backup_path,
    get_terminal_status_counts,
    highlight_status_cell,
    show_date_select,
    show_filter,
    show_sandbox_message,
)
from service.data_handling import get_combined_raw_files_and_metrics_df, get_lag_time
from service.db import get_full_raw_file_data
from service.utils import (
    APP_URL,
    DEFAULT_MAX_AGE_OVERVIEW,
    DEFAULT_MAX_TABLE_LEN,
    FILTER_MAPPING,
    QueryParams,
    _log,
    display_info_message,
    display_plotly_chart,
)

from shared.db.models import ERROR_STATUSES, TERMINAL_STATUSES

_log(f"loading {__file__}")


@dataclass
class Column:
    """Data class for information on how to display a column information."""

    name: str
    # hide column in table
    hide: bool = False
    # move column to front of table
    at_front: bool = False
    # move column to end of table
    at_end: bool = False
    # color gradient in table: None (no gradient), "green_is_high" (green=high, red=low), "red_is_high" (red=high, green=low)
    color_gradient: str | None = None
    # show as plot
    plot: bool = False
    # use log scale for plot
    log_scale: bool = False
    # alternative names in the database
    alternative_names: list[str] | None = None
    # optional plot
    plot_optional: bool = False


def _load_columns_from_yaml() -> tuple[Column, ...]:
    """Load column configuration from YAML file."""
    columns_config_file_path = Path(__file__).parent / ".." / "columns_config.yaml"

    with columns_config_file_path.open() as f:
        columns_config = yaml.safe_load(f)

    return tuple(
        [
            Column(
                name=column["name"],
                hide=column.get("hide"),
                at_front=column.get("at_front"),
                at_end=column.get("at_end"),
                color_gradient=column.get("color_gradient"),
                plot=column.get("plot"),
                log_scale=column.get("log_scale"),
                alternative_names=column.get("alternative_names"),
                plot_optional=column.get("plot_optional"),
            )
            for column in columns_config["columns"]
        ]
    )


COLUMNS = _load_columns_from_yaml()

# ########################################### PAGE HEADER

st.set_page_config(page_title="AlphaKraken: overview", layout="wide")

show_sandbox_message()

st.markdown("# Overview")

st.write(
    f"Current Kraken time: {datetime.now(tz=pytz.UTC).replace(microsecond=0)} [all time stamps are given in UTC!]"
)


days = 60
max_age_url = f"{APP_URL}/overview?max_age={days}"
st.markdown(
    f"""
    Note: for performance reasons, by default only data for the last {DEFAULT_MAX_AGE_OVERVIEW} days are loaded.
    If you want to see more data, use the `?max_age=` query parameter in the url, e.g.
    <a href="{max_age_url}" target="_self">{max_age_url}</a>
    """,
    unsafe_allow_html=True,
)

st.write(
    "Use the filter and date select to narrow down results both in the table and the plots below."
)

display_info_message()

# ########################################### LOGIC
max_age_in_days = float(
    st.query_params.get(QueryParams.MAX_AGE, DEFAULT_MAX_AGE_OVERVIEW)
)


def _harmonize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Harmonize the DataFrame by mapping all alternative names to their current ones."""
    names_mapping = {
        alternative_name: column.name
        for column in COLUMNS
        if column.alternative_names
        for alternative_name in column.alternative_names  # type: ignore[not-iterable]
        if column.alternative_names is not None
    }
    df = df.rename(columns=names_mapping)

    # map all columns of the same name to the first one, assuming that not more than one of the values are filled
    return df.groupby(axis=1, level=0).first()


with st.spinner("Loading data ..."):
    combined_df = get_combined_raw_files_and_metrics_df(max_age_in_days)
    combined_df = _harmonize_df(combined_df)


# ########################################### DISPLAY: table


columns_at_front = [column.name for column in COLUMNS if column.at_front]
columns_at_end = [column.name for column in COLUMNS if column.at_end] + [
    col for col in combined_df.columns if col.endswith("_std")
]
columns_to_hide = [column.name for column in COLUMNS if column.hide]


def _get_column_order(df: pd.DataFrame) -> list[str]:
    """Get column order."""
    return (
        columns_at_front
        + [
            col
            for col in df.columns
            if col not in columns_at_front + columns_at_end + columns_to_hide
        ]
        + columns_at_end
    )


def _filter_valid_columns(columns: list[str], df: pd.DataFrame) -> list[str]:
    """Filter out `columns` that are not in the `df`."""
    return [col for col in columns if col in df.columns]


@st.cache_data
def df_to_csv(df: pd.DataFrame) -> str:
    """Convert a DataFrame to a CSV string."""
    return df.to_csv().encode("utf-8")


# using a fragment to avoid re-doing the above operations on every filter change
# cf. https://docs.streamlit.io/develop/concepts/architecture/fragments
@st.experimental_fragment
def _display_table_and_plots(  # noqa: PLR0915,C901,PLR0912 (too many statements, too complex, too many branches)
    df: pd.DataFrame, max_age_in_days: float, filter_value: str = ""
) -> None:
    """A fragment that displays a DataFrame with a filter."""
    st.markdown("## Data")

    now = datetime.now(tz=pytz.UTC).replace(microsecond=0)
    st.text(f"Last fetched {now.strftime('%Y-%m-%d %H:%M:%S')}")

    # filter
    len_whole_df = len(df)
    c1, c2, _ = st.columns([0.5, 0.25, 0.25])

    filtered_df, user_input, filter_errors = show_filter(
        df,
        text_to_display="Filter:",
        st_display=c1,
        default_value=filter_value,
        example_text="astral1 & !hela & AlKr(.*)5ng & status=done & proteins=[400,500] & settings_version=1",
    )
    filtered_df = show_date_select(
        filtered_df,
        st_display=c2,
        max_age_days=9999
        if user_input
        else None,  # hacky way to always display all data if filter is set
    )

    if filter_errors:
        st.warning("\n".join(filter_errors))

    if user_input:
        encoded_user_input = user_input
        for key, value in FILTER_MAPPING.items():
            encoded_user_input = encoded_user_input.replace(" ", "").replace(
                value.strip(), key
            )

        url = f"{APP_URL}/overview?{QueryParams.FILTER}={encoded_user_input}"
        st.markdown(
            f"""Hint: save this filter by bookmarking <a href="{url}" target="_self">{url}</a>""",
            unsafe_allow_html=True,
        )

    max_table_len = int(
        st.query_params.get(QueryParams.MAX_TABLE_LEN, DEFAULT_MAX_TABLE_LEN)
    )
    st.write(
        f"Found {len(filtered_df)} / {len_whole_df} entries. Distribution of terminal statuses: {get_terminal_status_counts(filtered_df)} "
        f"Note: data is limited to last {max_age_in_days} days, table display is limited to first {max_table_len} entries. See FAQ how to change this.",
    )

    # Display lag time for latest done files
    lag_times, lag_time = get_lag_time(filtered_df)
    if lag_time:
        st.markdown(
            f"⏱️ Average lag time*: **{lag_time / 60:.1f} minutes** [*from start of acquisition to end of quanting, for last 10 'done' files in selection]"
        )

        filtered_df["lag_time_minutes"] = lag_times / 60
        filtered_df["eta"] = _add_eta(filtered_df, now, lag_time)

    # hide the csv download button to not encourage downloading incomplete data
    st.markdown(
        "<style>[data-testid='stElementToolbarButton']:first-of-type { display: none; } </style>",
        unsafe_allow_html=True,
    )
    # display only subset of entries to speed up page loading
    df_to_show = filtered_df.head(max_table_len)

    cmap = plt.get_cmap("RdYlGn")
    cmap_reversed = plt.get_cmap("RdYlGn_r")
    cmap.set_bad(color="white")
    cmap_reversed.set_bad(color="white")

    # Separate columns by gradient direction
    green_is_high_columns = _filter_valid_columns(
        [column.name for column in COLUMNS if column.color_gradient == "green_is_high"],
        filtered_df,
    )
    red_is_high_columns = _filter_valid_columns(
        [column.name for column in COLUMNS if column.color_gradient == "red_is_high"],
        filtered_df,
    )

    try:
        style = df_to_show.style

        # Apply green_is_high gradient (red=low, green=high)
        if green_is_high_columns:
            style = style.background_gradient(
                subset=green_is_high_columns,
                cmap=cmap,
            )

        # Apply red_is_high gradient (green=low, red=high)
        if red_is_high_columns:
            style = style.background_gradient(
                subset=red_is_high_columns,
                cmap=cmap_reversed,
            )

        st.dataframe(
            style.apply(highlight_status_cell, axis=1)
            .format(
                subset=list(filtered_df.select_dtypes(include=["float64"]).columns),
                formatter="{:.3}",
            )
            .format(
                subset=[
                    "settings_version",
                ],
                formatter="{:.0f}",
            ),
            column_order=_get_column_order(filtered_df),
        )
    except Exception as e:  # noqa: BLE001
        _log(e)
        st.dataframe(df_to_show)

    c1, _ = st.columns([0.5, 0.5])
    with c1.expander("Click here for help ..."):
        st.info(
            """
            #### Explanation of 'status' information
            - `done`: The file has been fully processed successfully.
            - `done_not_quanted`: The file has been handled successfully, but was not quanted (check the "status_details" column).
            - `acquisition_failed`: the acquisition of the file failed (check the "status_details" column).
            - `quanting_failed`: something went wrong with the quanting, check the "status_details" column for more information:
              - `NO_RECALIBRATION_TARGET`: alphaDIA did not find enough precursors to calibrate the data.
              - `NOT_DIA_DATA`: the file is not DIA data.
              - `TIMEOUT`: the quanting job took too long and was stopped
              - `_*`: a underscore as prefix indicates a known error, whose root cause has not been investigated yet.
              - `__*`: a double underscore as prefix indicates that there was an error while investigating the error.
            - `error`: an unknown error happened during processing, check the "status_details" column for more information
                and report it to the developers if unsure.

            All other states are transient and should be self-explanatory. If you feel a file stays in a certain status
            for too long, please report it to the developers.
        """,
            icon="ℹ️",  # noqa: RUF001
        )

    # ########################################### DISPLAY: Download buttons

    c1, c2, _ = st.columns([0.25, 0.25, 0.5])
    c1.download_button(
        label=f"⬇️ Download filtered table ({len(filtered_df)} entries)",
        data=df_to_csv(filtered_df),
        file_name=f"{now.strftime('AlphaKraken_%Y%m%d-%H%M%S_filtered')}.csv",
        mime="text/csv",
    )

    c2.download_button(
        label=f"⬇️ Download all data ({len(df)} entries)",
        data=df_to_csv(df),
        file_name=f"{now.strftime('AlphaKraken_%Y%m%d-%H%M%S_all')}.csv",
        mime="text/csv",
    )

    # ########################################### DISPLAY: Files

    st.markdown("## Files")
    c1, c2, _ = st.columns([0.10, 0.10, 0.5])
    prefix = (
        " - "
        if c2.checkbox(
            "AlphaDIA-compatible prefix",
            help="Whether the Multi-line format should carry a hyphen as prefix",
        )
        else ""
    )
    if c1.button(
        "Show file paths for selection",
        help="For the selection in the table, show all file paths on the backup for conveniently copying them manually to another location.",
    ):
        full_info_df = get_full_raw_file_data(list(filtered_df.index))
        file_paths, is_multiple_types = get_full_backup_path(full_info_df)

        with st.expander(f"Found {len(file_paths)} items:", expanded=True):
            if is_multiple_types:
                st.warning(
                    "Warning: more than one instrument type found, please check your selection!"
                )

            st.write("One-line format:")
            file_paths_pretty_one_line = " ".join(file_paths)
            st.code(f"{file_paths_pretty_one_line}")

            st.write("Multi-line format:")
            file_paths_pretty = f"\n{prefix}".join(file_paths)
            st.code(f"{prefix}{file_paths_pretty}")

    # ########################################### DISPLAY: plots

    st.markdown("## Plots")

    st.info(
        "If you don't see any data points try reducing the number by filtering, and/or use Firefox!"
    )

    c1, c2, c3, c4 = st.columns([0.25, 0.25, 0.25, 0.25])
    column_order = _get_column_order(filtered_df)
    color_by_column = c1.selectbox(
        label="Color by:",
        options=["instrument_id"]
        + [col for col in column_order if col != "instrument_id"],
        help="Choose the column to color by.",
    )
    x = c2.selectbox(
        label="Choose x-axis:",
        options=["file_created"]
        + [col for col in column_order if col != "file_created"],
        help="Set the x-axis. The default 'file_created' is suitable for most cases.",
    )
    show_traces = c3.checkbox(
        label="Show traces",
        value=True,
        help="Show traces for each data point.",
    )
    show_std = c4.checkbox(
        label="Show standard deviations",
        value=False,
        help="Show standard deviations for mean values.",
    )

    for column in [
        column
        for column in COLUMNS
        if (column.plot and column.name in filtered_df.columns)
    ]:
        try:
            _draw_plot(
                filtered_df,
                x=x,
                column=column,
                color_by_column=color_by_column,
                show_traces=show_traces,
                show_std=show_std,
            )
        except Exception as e:  # noqa: BLE001, PERF203
            if not column.plot_optional:
                _log(e, f"Cannot draw plot for {column.name} vs {x}.")
            else:
                st.write("n/a")


def _add_eta(df: pd.DataFrame, now: datetime, lag_time: float) -> pd.Series:
    """Return the "ETA" column for the dataframe."""
    # TODO: this would become more precises if lag times would be calculated per instrument & project
    non_terminal_mask = ~df["status"].isin(TERMINAL_STATUSES)
    eta_timestamps = (
        df.loc[non_terminal_mask, "created_at_"] + pd.Timedelta(seconds=lag_time)
    ).dt.tz_localize("UTC")

    # Convert ETA timestamps to human-readable format showing "in X time"
    def _format_eta(eta_time: datetime) -> str:
        """Format the eta time to a string."""
        time_diff = eta_time.replace(microsecond=0) - now
        if eta_time <= now:
            return f"now ({time_diff})"
        return get_display_time(now - time_diff, now, prefix="in ", suffix="")

    return eta_timestamps.apply(_format_eta)


def _draw_plot(  # noqa: PLR0913
    df: pd.DataFrame,
    *,
    x: str,
    column: Column,
    color_by_column: str,
    show_traces: bool,
    show_std: bool,
) -> None:
    """Draw a plot of a DataFrame."""
    df = df.sort_values(by=x)

    y = column.name
    y_is_numeric = pd.api.types.is_numeric_dtype(df[y])
    median_ = df[y].median() if y_is_numeric else 0
    title = f"{y} (median= {median_:.2f})" if y_is_numeric else y

    hover_data = _filter_valid_columns(
        [
            "file_created",
            "size_gb",
            "precursors",
            "status",
        ],
        df,
    )

    if y == "status" and "status_details" in df:
        df.loc[pd.isna(df["status_details"]), "status_details"] = ""
        hover_data.append("status_details")

    fig = px.scatter(
        df,
        x=x,
        y=y,
        color=color_by_column,
        hover_name="_id",
        hover_data=hover_data,
        title=title,
        height=400,
        error_y=_get_yerror_column_name(y, df) if show_std else None,
        log_y=column.log_scale,
    )
    if y_is_numeric and show_traces:
        symbol = [
            "x" if x in ERROR_STATUSES else "circle" for x in df["status"].to_numpy()
        ]
        fig.update_traces(mode="lines+markers", marker={"symbol": symbol})
    fig.add_hline(y=median_, line_dash="dash", line={"color": "lightgrey"})
    display_plotly_chart(fig)


def _get_yerror_column_name(y_column_name: str, df: pd.DataFrame) -> str | None:
    """Get the name of the error column for `y_column_name`, if it endwith '_mean' and is available in the `df`."""
    if not y_column_name.endswith("_mean"):
        return None

    if (yerror_column_name := y_column_name.replace("_mean", "_std")) not in df.columns:
        return None

    return yerror_column_name


filter_value = st.query_params.get(QueryParams.FILTER, "")
for key_, value_ in FILTER_MAPPING.items():
    filter_value = filter_value.lower().replace(key_.lower(), value_)

_display_table_and_plots(
    combined_df,
    max_age_in_days,
    filter_value,
)
