"""Simple data overview."""

from dataclasses import dataclass
from datetime import datetime

import pandas as pd
import plotly.express as px
import pytz

# ruff: noqa: PD002 # `inplace=True` should be avoided; it has inconsistent behavior
import streamlit as st
from matplotlib import pyplot as plt
from service.components import (
    get_terminal_status_counts,
    highlight_status_cell,
    show_date_select,
    show_filter,
    show_sandbox_message,
)
from service.data_handling import get_combined_raw_files_and_metrics_df
from service.utils import (
    APP_URL,
    DEFAULT_MAX_AGE_OVERVIEW,
    DEFAULT_MAX_TABLE_LEN,
    ERROR_STATUSES,
    FILTER_MAPPING,
    QueryParams,
    _log,
    display_info_message,
)

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
    # color in table
    color_table: bool = False
    # show as plot
    plot: bool = False
    # use log scale for plot
    log_scale: bool = False


COLUMNS = (
    # hide
    Column("created_at", hide=True),
    Column("size", hide=True),
    Column("quanting_time_elapsed", hide=True),
    Column("raw_file", hide=True),
    Column("file_info", hide=True),
    Column("_id", hide=True),
    Column("original_name", hide=True),
    Column("collision_flag", hide=True),
    # at front (order matters)
    Column("instrument_id", at_front=True),
    Column("status", at_front=True),
    Column("status_details", at_front=True),
    Column("size_gb", at_front=True, color_table=True, plot=True),
    Column("file_created", at_front=True),
    # at end (order matters)
    Column("project_id", at_end=True),
    Column("updated_at_", at_end=True),
    Column("created_at_", at_end=True),
    # plots (order matters)
    Column("precursors", color_table=True, plot=True),
    Column("proteins", color_table=True, plot=True),
    Column("ms1_accuracy", color_table=True, plot=True),
    Column("fwhm_rt", color_table=True, plot=True),
    Column("weighted_ms1_intensity_sum", color_table=True, plot=True),
    Column("intensity_sum", color_table=True, plot=True, log_scale=True),
    Column("settings_version", at_end=True, plot=True),
    Column("quanting_time_minutes", color_table=True, plot=True),
    Column("duration_optimization", color_table=True, plot=True, at_end=True),
    Column("duration_extraction", color_table=True, plot=True, at_end=True),
    Column("ms1_error", color_table=True, plot=True),
    Column("ms2_error", color_table=True, plot=True),
    Column("rt_error", color_table=True, plot=True),
    Column("mobility_error", color_table=True, plot=True),
)

# ########################################### PAGE HEADER

st.set_page_config(page_title="AlphaKraken: overview", layout="wide")

show_sandbox_message()

st.markdown("# Overview")

st.write(
    f"Current Kraken time: {datetime.now(tz=pytz.UTC).replace(microsecond=0)} [all time stamps are given in UTC!]"
)


days = 60
url = f"{APP_URL}/overview?max_age={days}"
st.markdown(
    f"""
    Note: for performance reasons, by default only data for the last {DEFAULT_MAX_AGE_OVERVIEW} days are loaded.
    If you want to see more data, use the `?max_age=` query parameter in the url, e.g.
    <a href="{url}" target="_self">{url}</a>
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
with st.spinner("Loading data ..."):
    combined_df = get_combined_raw_files_and_metrics_df(max_age_in_days)


# ########################################### DISPLAY: table


columns_at_front = [column.name for column in COLUMNS if column.at_front]
columns_at_end = [column.name for column in COLUMNS if column.at_end]
columns_to_hide = [column.name for column in COLUMNS if column.hide]

column_order = (
    columns_at_front
    + [
        col
        for col in combined_df.columns
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
def _display_table_and_plots(
    df: pd.DataFrame, max_age_in_days: float, filter_value: str = ""
) -> None:
    """A fragment that displays a DataFrame with a filter."""
    st.markdown("## Data")
    now = datetime.now(tz=pytz.UTC)

    # filter
    len_whole_df = len(df)
    c1, c2, _ = st.columns([0.5, 0.25, 0.25])

    filtered_df, user_input, filter_errors = show_filter(
        df,
        text_to_display="Filter:",
        st_display=c1,
        default_value=filter_value,
        placeholder="E.g. `test2 & !hela & MaSc(*.)5ng & status=done`",
    )
    filtered_df = show_date_select(
        filtered_df,
        st_display=c2,
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

    # hide the csv download button to not encourage downloading incomplete data
    st.markdown(
        "<style>[data-testid='stElementToolbarButton']:first-of-type { display: none; } </style>",
        unsafe_allow_html=True,
    )
    # display only subset of entries to speed up page loading
    df_to_show = filtered_df.head(max_table_len)

    cmap = plt.get_cmap("Blues")
    cmap.set_bad(color="white")
    st.dataframe(
        df_to_show.style.background_gradient(
            subset=_filter_valid_columns(
                [column.name for column in COLUMNS if column.color_table],
                filtered_df,
            ),
            cmap=cmap,
        )
        .apply(highlight_status_cell, axis=1)
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
        column_order=column_order,
    )

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
        file_name=f'{now.strftime("AlphaKraken_%Y%m%d-%H%M%S_filtered")}.csv',
        mime="text/csv",
    )

    c2.download_button(
        label=f"⬇️ Download all data ({len(df)} entries)",
        data=df_to_csv(df),
        file_name=f'{now.strftime("AlphaKraken_%Y%m%d-%H%M%S_all")}.csv',
        mime="text/csv",
    )

    # ########################################### DISPLAY: plots

    st.markdown("## Plots")
    selectbox_columns = ["file_created"] + [
        col for col in column_order if col != "file_created"
    ]
    c1, _ = st.columns([0.25, 0.75])
    x = c1.selectbox(
        label="Choose x-axis:",
        options=selectbox_columns,
        help="Set the x-axis. The default 'file_created' is suitable for most cases.",
    )
    for column in [
        column
        for column in COLUMNS
        if (column.plot and column.name in filtered_df.columns)
    ]:
        try:
            _draw_plot(filtered_df, x, column)
        except Exception as e:  # noqa: BLE001, PERF203
            _log(e, f"Cannot draw plot for {column.name} vs {x}.")


def _draw_plot(df: pd.DataFrame, x: str, column: Column) -> None:
    """Draw a plot of a DataFrame."""
    df = df.sort_values(by=x)

    y = column.name
    y_is_numeric = pd.api.types.is_numeric_dtype(df[y])
    median_ = df[y].median() if y_is_numeric else 0
    title = f"{y} (median= {median_:.2f})" if y_is_numeric else y

    hover_data = [
        "file_created",
        "size_gb",
        "precursors",
        "status",
    ]

    if y == "status" and "status_details" in df:
        df.loc[pd.isna(df["status_details"]), "status_details"] = ""
        hover_data.append("status_details")

    fig = px.scatter(
        df,
        x=x,
        y=y,
        color="instrument_id",
        hover_name="_id",
        hover_data=hover_data,
        title=title,
        height=400,
        error_y=_get_yerror_column_name(y, df),
        log_y=column.log_scale,
    )
    if y_is_numeric:
        symbol = [
            "x" if x in ERROR_STATUSES else "circle" for x in df["status"].to_numpy()
        ]
        fig.update_traces(
            mode="lines+markers",
            marker={"symbol": symbol},
        )
    fig.add_hline(y=median_, line_dash="dash", line={"color": "lightgrey"})
    st.plotly_chart(fig)


def _get_yerror_column_name(y_column_name: str, df: pd.DataFrame) -> str | None:
    """Get the name of the error column for `y_column_name`, if it endwith '_mean' and is available in the `df`."""
    if not y_column_name.endswith("_mean"):
        return None

    if (yerror_column_name := y_column_name.replace("_mean", "_std")) not in df.columns:
        return None

    return yerror_column_name


filter_value = st.query_params.get(QueryParams.FILTER, "")
for key, value in FILTER_MAPPING.items():
    filter_value = filter_value.lower().replace(key.lower(), value)

_display_table_and_plots(
    combined_df,
    max_age_in_days,
    filter_value,
)
