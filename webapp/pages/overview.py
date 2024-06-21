"""Simple data overview."""

import pandas as pd
import plotly.express as px

# ruff: noqa: PD002 # `inplace=True` should be avoided; it has inconsistent behavior
import streamlit as st
from matplotlib import pyplot as plt
from service.components import display_status, show_date_select, show_filter
from service.db import df_from_db_data, get_raw_file_and_metrics_data
from service.utils import _log

_log(f"loading {__file__}")


# ########################################### PAGE HEADER

st.set_page_config(page_title="AlphaKraken: overview", layout="wide")
st.markdown("# Overview")

# ########################################### LOGIC

raw_files_db, metrics_db = get_raw_file_and_metrics_data()

raw_files_df = df_from_db_data(raw_files_db)

metrics_df = df_from_db_data(
    metrics_db,
    drop_duplicates=["raw_file"],
    drop_columns=["_id", "created_at_"],
)

if len(raw_files_df) == 0 or len(metrics_df) == 0:
    st.write(f"No data found yet: {len(raw_files_df)=} {len(metrics_df)=}.")
    st.stop()

# the joining could also be done on DB level
combined_df = raw_files_df.merge(
    metrics_df, left_on="_id", right_on="raw_file", how="left"
)

# conversions
combined_df["size_gb"] = combined_df["size"] / 1024**3
combined_df["file_created"] = combined_df["created_at"].dt.strftime("%Y-%m-%d %H:%M:%S")
combined_df["quanting_time_minutes"] = combined_df["quanting_time_elapsed"] / 60

# eye candy
combined_df.sort_values(by="created_at", ascending=False, inplace=True)
combined_df.reset_index(drop=True, inplace=True)
combined_df.index = combined_df["_id"]
combined_df.drop(
    columns=["size", "quanting_time_elapsed", "raw_file", "_id"], inplace=True
)
columns_at_end = ["created_at", "created_at_", "updated_at_"]
combined_df = combined_df[
    [col for col in combined_df.columns if col not in columns_at_end] + columns_at_end
]


# ########################################### DISPLAY: table


# using a fragment to avoid re-doing the above operations on every filter change
# cf. https://docs.streamlit.io/develop/concepts/architecture/fragments
@st.experimental_fragment
def display(df: pd.DataFrame) -> None:
    """A fragment that displays a DataFrame with a filter."""
    st.markdown("## Status")
    try:
        display_status(df)
    except Exception as e:  # noqa: BLE001
        _log(str(e))
        st.warning(f"Cannot not display status: {e}.")

    st.markdown("## Data")

    # filter
    len_whole_df = len(df)
    c1, c2 = st.columns([0.7, 0.3])
    filtered_df = show_filter(df, text_to_display="Filter:", st_display=c1)
    filtered_df = show_date_select(
        filtered_df,
        st_display=c2,
    )

    st.write(f"Showing {len(filtered_df)} / {len_whole_df} entries.")

    cmap = plt.get_cmap("RdYlGn")
    st.dataframe(
        filtered_df.style.background_gradient(
            subset=[
                "BasicStats_proteins_mean",
            ],
            cmap=cmap,
        )
    )

    # ########################################### DISPLAY: plots

    st.markdown("## Plots")
    x = "file_created"
    for y in [
        "size_gb",
        "BasicStats_precursors_mean",
        "BasicStats_proteins_mean",
        "BasicStats_ms1_accuracy_mean",
        "BasicStats_fwhm_rt_mean",
        "quanting_time_minutes",
    ]:
        try:
            draw_plot(filtered_df, x, y)
        except Exception as e:  # noqa: BLE001, PERF203
            _log(str(e))


def draw_plot(df: pd.DataFrame, x: str, y: str) -> None:
    """Draw a plot of a DataFrame."""
    df_to_plot = df.reset_index()
    median_ = df_to_plot[y].median()
    fig = px.scatter(
        df_to_plot,
        x=x,
        y=y,
        color="instrument_id",
        hover_name="_id",
        hover_data=["file_created"],
        title=f"{y} - median {median_:.2f}",
        height=400,
    ).update_traces(mode="lines+markers")
    fig.add_hline(y=median_, line_dash="dash")
    st.plotly_chart(fig)


display(combined_df)
