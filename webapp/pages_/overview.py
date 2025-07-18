"""Simple data overview."""

from datetime import datetime
from functools import partial

import pandas as pd
import pytz

# ruff: noqa: PD002 # `inplace=True` should be avoided; it has inconsistent behavior
import streamlit as st
from matplotlib import pyplot as plt
from pages_.impl.overview_plotting import _draw_plot
from pages_.impl.overview_utils import (
    add_eta,
    df_to_csv,
    filter_valid_columns,
    get_column_order,
    harmonize_df,
    load_columns_from_yaml,
)
from service.components import (
    get_full_backup_path,
    get_terminal_status_counts,
    highlight_status_cell,
    show_date_select,
    show_filter,
    show_sandbox_message,
)
from service.data_handling import get_combined_raw_files_and_metrics_df, get_lag_time
from service.db import get_full_raw_file_data, get_raw_file_and_metrics_data
from service.session_state import (
    SessionStateKeys,
    copy_session_state,
    get_session_state,
)
from service.status import show_status_warning
from service.utils import (
    APP_URL,
    DEFAULT_MAX_AGE_OVERVIEW,
    DEFAULT_MAX_TABLE_LEN,
    FILTER_MAPPING,
    Cols,
    QueryParams,
    _log,
    display_info_message,
)

BASELINE_PREFIX = "BASELINE_"

_log(f"loading {__file__} {st.query_params}")


COLUMNS = load_columns_from_yaml()

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
    If you want to see more data, use the "?max_age=" query parameter in the url, e.g.
    <a href="{max_age_url}" target="_self">{max_age_url}</a>
    """,
    unsafe_allow_html=True,
)

st.write(
    "Use the filter and date select to narrow down results both in the table and the plots below."
)

display_info_message()


show_status_warning()


# ########################################### LOGIC
max_age_in_days = float(
    st.query_params.get(QueryParams.MAX_AGE, DEFAULT_MAX_AGE_OVERVIEW)
)


with st.spinner("Loading data ..."):
    combined_df, data_timestamp = get_combined_raw_files_and_metrics_df(
        max_age_in_days=max_age_in_days, stop_at_no_data=True
    )
    combined_df = harmonize_df(combined_df, COLUMNS)
    combined_df[Cols.IS_BASELINE] = False

    # Load and merge baseline data if specified
    baseline_raw_files = st.query_params.get(QueryParams.BASELINE, "")
    if baseline_raw_files:
        baseline_file_names = [name.strip() for name in baseline_raw_files.split(",")]
        baseline_df, _ = get_combined_raw_files_and_metrics_df(
            raw_file_ids=baseline_file_names
        )

        baseline_df[Cols.IS_BASELINE] = True
        baseline_df = harmonize_df(baseline_df, COLUMNS)
        # this is a hack to prevent index clashing
        baseline_df.index = [BASELINE_PREFIX + str(idx) for idx in baseline_df.index]
        combined_df = pd.concat([combined_df, baseline_df], ignore_index=False)

# ########################################### DISPLAY: table

known_columns = [
    column.name for column in COLUMNS if column.name in combined_df.columns
]
columns_at_end = [column.name for column in COLUMNS if column.at_end] + [
    col for col in combined_df.columns if col.endswith("_std")
]
columns_to_hide = [column.name for column in COLUMNS if column.hide]


# using a fragment to avoid re-doing the above operations on every filter change
# cf. https://docs.streamlit.io/develop/concepts/architecture/fragments
@st.fragment
def _display_table_and_plots(  # noqa: PLR0915,C901,PLR0912 (too many statements, too complex, too many branches)
    df: pd.DataFrame,
    max_age_in_days: float,
    filter_value: str,
    data_timestamp: datetime,
) -> None:
    """A fragment that displays a DataFrame with a filter."""
    st.markdown("## Data")

    st.text(f"Last fetched {data_timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
    if st.button("🔄 Refresh"):
        get_raw_file_and_metrics_data.clear()
        st.rerun()

    # filter
    len_whole_df = len(df)
    c1, c2, _ = st.columns([0.5, 0.25, 0.25])

    filtered_df, user_input, filter_errors = show_filter(
        df,
        text_to_display="Filter:",
        st_display=c1,
        default_value=None if filter_value.strip() == "" else filter_value,
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
        for param in [QueryParams.MOBILE, QueryParams.MAX_AGE, QueryParams.BASELINE]:
            if param in st.query_params:
                url += f"&{param}={st.query_params[param]}"

        url = url.replace(" ", "")

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
        filtered_df["eta"] = add_eta(
            filtered_df, datetime.now(tz=pytz.UTC).replace(microsecond=0), lag_time
        )

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
    green_is_high_columns = filter_valid_columns(
        [column.name for column in COLUMNS if column.color_gradient == "green_is_high"],
        filtered_df,
    )
    red_is_high_columns = filter_valid_columns(
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
            column_order=get_column_order(filtered_df, COLUMNS),
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
        file_name=f"{data_timestamp.strftime('AlphaKraken_%Y%m%d-%H%M%S_filtered')}.csv",
        mime="text/csv",
    )

    c2.download_button(
        label=f"⬇️ Download all data ({len(df)} entries)",
        data=df_to_csv(df),
        file_name=f"{data_timestamp.strftime('AlphaKraken_%Y%m%d-%H%M%S_all')}.csv",
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

    if filtered_df[Cols.IS_BASELINE].any():
        baseline_samples = filtered_df[filtered_df[Cols.IS_BASELINE]].index.to_list()
        baseline_samples_str = ", ".join(
            [s[len(BASELINE_PREFIX) :] for s in baseline_samples]
        )
        st.info(
            f"Showing baseline data (mean ± std as green lines) for {len(baseline_samples)} samples:\n{baseline_samples_str} "
        )

    c1, c2, c3, c4, c5, c6 = st.columns([0.16, 0.16, 0.16, 0.16, 0.16, 0.16])
    column_order = get_column_order(filtered_df, COLUMNS)

    color_by_column = c1.selectbox(
        label="Color by:",
        options=["instrument_id"]
        + [col for col in column_order if col != "instrument_id"],
        help="Choose the column to color by.",
    )  # TODO: state not persisted on 'refresh'

    x = c2.selectbox(
        label="Choose x-axis:",
        options=["file_created"]
        + [col for col in column_order if col != "file_created"],
        help="Set the x-axis. The default 'file_created' is suitable for most cases.",
    )  # TODO: state not persisted on 'refresh'

    show_traces = c3.checkbox(
        label="Show traces",
        value=get_session_state(SessionStateKeys.SHOW_TRACES, default=True),
        help="Show traces for each data point.",
        key="show_traces_widget_key",
        on_change=partial(
            copy_session_state, SessionStateKeys.SHOW_TRACES, "show_traces_widget_key"
        ),
    )

    show_std = c4.checkbox(
        label="Show standard deviations",
        value=get_session_state(SessionStateKeys.SHOW_STD, default=False),
        help="Show standard deviations for mean values.",
        key="show_std_widget_key",
        on_change=partial(
            copy_session_state, SessionStateKeys.SHOW_STD, "show_std_widget_key"
        ),
    )

    show_trendline = c5.checkbox(
        label="Show trendlines",
        value=get_session_state(SessionStateKeys.SHOW_TRENDLINE, default=False),
        help="Show linear regression trendlines for numeric data.",
        key="show_trendline_widget_key",
        on_change=partial(
            copy_session_state,
            SessionStateKeys.SHOW_TRENDLINE,
            "show_trendline_widget_key",
        ),
    )

    plots_per_row = c6.selectbox(
        label="Plots per row:",
        options=[r + 1 for r in range(9)],
        help="Number of plots to display per row.",
        index=plots_per_row - 1
        if (plots_per_row := get_session_state(SessionStateKeys.PLOTS_PER_ROW))
        is not None
        else 0,
        key="plots_per_row_widget_key",
        on_change=partial(
            copy_session_state,
            SessionStateKeys.PLOTS_PER_ROW,
            "plots_per_row_widget_key",
        ),
    )

    columns_to_plot = [
        column
        for column in COLUMNS
        if (column.plot and column.name in filtered_df.columns)
    ]

    # Group plots by rows based on plots_per_row setting
    for row_idx in range(0, len(columns_to_plot), plots_per_row):
        row_columns = columns_to_plot[row_idx : row_idx + plots_per_row]
        st_columns = st.columns(len(row_columns))

        for col_idx, column in enumerate(row_columns):
            try:
                with st_columns[col_idx]:
                    _draw_plot(
                        filtered_df,
                        x=x,
                        column=column,
                        color_by_column=color_by_column,
                        show_traces=show_traces,
                        show_std=show_std,
                        show_trendline=show_trendline,
                    )
            except Exception as e:  # noqa: BLE001, PERF203
                if not column.plot_optional:
                    _log(e, f"Cannot draw plot for {column.name} vs {x}.")
                else:
                    with st_columns[col_idx]:
                        st.write("n/a")


filter_value = st.query_params.get(QueryParams.FILTER, "")
for key_, value_ in FILTER_MAPPING.items():
    filter_value = filter_value.lower().replace(key_.lower(), value_)


_display_table_and_plots(combined_df, max_age_in_days, filter_value, data_timestamp)
