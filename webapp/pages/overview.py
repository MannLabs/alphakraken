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
from service.utils import ERROR_STATUSES, _log

_log(f"loading {__file__}")


# ########################################### PAGE HEADER

st.set_page_config(page_title="AlphaKraken: overview", layout="wide")

show_sandbox_message()

st.markdown("# Overview")

st.write(
    f"Current Kraken time: {datetime.now(tz=pytz.UTC).replace(microsecond=0)} [all time stamps are given in UTC!]"
)
st.write(
    "Use the filter and date select to narrow down results both in the table and the plots below."
)

# ########################################### LOGIC

combined_df = get_combined_raw_files_and_metrics_df()


# ########################################### DISPLAY: table

columns_at_end = [
    "status_details",
    "project_id",
    "updated_at_",
    "created_at_",
    "file_info",
]
columns_to_hide = [
    "created_at",
    "size",
    "quanting_time_elapsed",
    "raw_file",
    "_id",
    "original_name",
    "collision_flag",
]
column_order = [
    col
    for col in combined_df.columns
    if col not in columns_at_end and col not in columns_to_hide
] + columns_at_end


# using a fragment to avoid re-doing the above operations on every filter change
# cf. https://docs.streamlit.io/develop/concepts/architecture/fragments
@st.experimental_fragment
def _display_table_and_plots(df: pd.DataFrame) -> None:
    """A fragment that displays a DataFrame with a filter."""
    st.markdown("## Data")

    # filter
    len_whole_df = len(df)
    c1, c2, _ = st.columns([0.5, 0.125, 0.375])
    filtered_df = show_filter(df, text_to_display="Filter:", st_display=c1)
    filtered_df = show_date_select(
        filtered_df,
        st_display=c2,
    )

    st.write(
        f"Showing {len(filtered_df)} / {len_whole_df} entries. Distribution of terminal statuses: {get_terminal_status_counts(combined_df)}"
    )

    cmap = plt.get_cmap("RdYlGn")
    cmap.set_bad(color="white")
    st.dataframe(
        filtered_df.style.background_gradient(
            subset=[
                "size_gb",
                "proteins",
                "precursors",
                "ms1_accuracy",
                "fwhm_rt",
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
                "quanting_time_minutes",
            ],
            formatter="{:.3}",
        ),
        column_order=column_order,
    )

    c1, _ = st.columns([0.5, 0.5])
    with c1.expander("Click here for help ..."):
        st.info(
            """
            #### Explanation of 'status' information
            - `done`: The file has been processed successfully.
            - `acquisition_failed`: the acquisition of the file failed, check the "status_details" column for more information.
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

    # ########################################### DISPLAY: plots

    st.markdown("## Plots")
    selectbox_columns = ["file_created"] + [
        col for col in column_order if col != "file_created"
    ]
    c1, _ = st.columns([0.125, 0.875])
    x = c1.selectbox(
        label="Choose x-axis:",
        options=selectbox_columns,
        help="Set the x-axis. Only required in special cases.",
    )
    for y in [
        "status",
        "size_gb",
        "precursors",
        "proteins",
        "ms1_accuracy",
        "fwhm_rt",
        "quanting_time_minutes",
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

    fig = px.scatter(
        df,
        x=x,
        y=y,
        color="instrument_id",
        hover_name="_id",
        hover_data=["file_created", "status", "size_gb", "precursors"],
        title=f"{y} (median= {median_:.2f})",
        height=400,
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


_display_table_and_plots(combined_df)
