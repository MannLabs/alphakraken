"""Simple data overview."""

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
    DEFAULT_MAX_AGE_OVERVIEW,
    DEFAULT_MAX_TABLE_LEN,
    ERROR_STATUSES,
    QueryParams,
    _log,
    display_info_message,
)

_log(f"loading {__file__}")


# ########################################### PAGE HEADER

st.set_page_config(page_title="AlphaKraken: overview", layout="wide")

show_sandbox_message()

st.markdown("# Overview")

st.write(
    f"Current Kraken time: {datetime.now(tz=pytz.UTC).replace(microsecond=0)} [all time stamps are given in UTC!]"
)

# TODO: remove this hack once https://github.com/streamlit/streamlit/issues/8112 is available
app_path = "http://<kraken_url>"
days = 60
st.markdown(
    f"""
    Note: for performance reasons, by default only data for the last {DEFAULT_MAX_AGE_OVERVIEW} days are loaded.
    If you want to see more data, use the `?max_age=` query parameter in the url, e.g.
    <a href="{app_path}/overview?max_age={days}" target="_self">{app_path}/overview?max_age={days}</a>
    """,
    unsafe_allow_html=True,
)

st.write(
    "Use the filter and date select to narrow down results both in the table and the plots below."
)

display_info_message()

# ########################################### LOGIC
max_age_in_days = int(
    st.query_params.get(QueryParams.MAX_AGE, DEFAULT_MAX_AGE_OVERVIEW)
)
combined_df = get_combined_raw_files_and_metrics_df(max_age_in_days)


# ########################################### DISPLAY: table

columns_at_end = [
    "settings_version",
    "status_details",
    "project_id",
    "updated_at_",
    "created_at_",
]
columns_to_hide = [
    "created_at",
    "size",
    "quanting_time_elapsed",
    "raw_file",
    "file_info",
    "_id",
    "original_name",
    "collision_flag",
]
column_order = [
    col
    for col in combined_df.columns
    if col not in columns_at_end and col not in columns_to_hide
] + columns_at_end


@st.cache_data
def df_to_csv(df: pd.DataFrame) -> str:
    """Convert a DataFrame to a CSV string."""
    return df.to_csv().encode("utf-8")


# using a fragment to avoid re-doing the above operations on every filter change
# cf. https://docs.streamlit.io/develop/concepts/architecture/fragments
@st.experimental_fragment
def _display_table_and_plots(
    df: pd.DataFrame, max_age_in_days: int, filter_value: str = ""
) -> None:
    """A fragment that displays a DataFrame with a filter."""
    st.markdown("## Data")
    now = datetime.now(tz=pytz.UTC)

    # filter
    len_whole_df = len(df)
    c1, c2, _ = st.columns([0.5, 0.25, 0.25])

    filtered_df = show_filter(
        df, text_to_display="Filter:", st_display=c1, default_value=filter_value
    )
    filtered_df = show_date_select(
        filtered_df,
        st_display=c2,
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

    cmap = plt.get_cmap("RdYlGn")
    cmap.set_bad(color="white")
    st.dataframe(
        df_to_show.style.background_gradient(
            subset=[
                "size_gb",
                "proteins",
                "precursors",
                "ms1_accuracy",
                "fwhm_rt",
                "weighted_ms1_intensity_mean",
                "quanting_time_minutes",
            ],
            cmap=cmap,
        )
        .apply(highlight_status_cell, axis=1)
        .format(
            subset=[
                "size_gb",
                "ms1_accuracy",
                "fwhm_rt",
                "weighted_ms1_intensity_mean",
                "quanting_time_minutes",
            ],
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
    for y in [
        "status",
        "size_gb",
        "precursors",
        "proteins",
        "ms1_accuracy",
        "fwhm_rt",
        "weighted_ms1_intensity_sum",
        "quanting_time_minutes",
        "settings_version",

    ]:
        try:
            _draw_plot(filtered_df, x, y)
        except Exception as e:  # noqa: BLE001, PERF203
            _log(e, f"Cannot draw plot for {y} vs {x}.")


def _draw_plot(df: pd.DataFrame, x: str, y: str) -> None:
    """Draw a plot of a DataFrame."""
    df = df.sort_values(by=x)

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
        error_y=None if not y.endswith("_mean") else y.replace("_mean", "_std"),
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


filter_value = (
    st.query_params.get(QueryParams.FILTER, "")
    .replace("AND", " & ")
    .replace("and", " & ")
)

_display_table_and_plots(
    combined_df,
    max_age_in_days,
    filter_value,
)
