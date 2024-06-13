"""Simple data overview."""

import pandas as pd

# ruff: noqa: PD002 # `inplace=True` should be avoided; it has inconsistent behavior
import streamlit as st
from matplotlib import pyplot as plt
from service.components import show_filter
from service.db import df_from_db_data, get_all_data
from service.utils import _log

_log("loading overview.py")


# ########################################### PAGE HEADER

st.set_page_config(page_title="AlphaKraken: overview", layout="wide")
st.markdown("# Overview")


# ########################################### LOGIC

raw_files_db, metrics_db = get_all_data()

raw_files_df = df_from_db_data(raw_files_db)

metrics_df = df_from_db_data(
    metrics_db,
    drop_duplicates=["raw_file"],
    drop_columns=["_id", "db_entry_created_at"],
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
combined_df["quanting_time_minutes"] = combined_df["time_elapsed"] / 60

# eye candy
combined_df.sort_values(by="created_at", ascending=False, inplace=True)
combined_df.reset_index(drop=True, inplace=True)
combined_df.index = combined_df["_id"]
combined_df.drop(
    columns=["size", "created_at", "time_elapsed", "raw_file", "_id"], inplace=True
)
combined_df = combined_df[
    [col for col in combined_df.columns if col != "db_entry_created_at"]
    + ["db_entry_created_at"]
]


# ########################################### DISPLAY


# using a fragment to avoid re-doing the above operations on every filter change
# cf. https://docs.streamlit.io/develop/concepts/architecture/fragments
@st.experimental_fragment
def display(df: pd.DataFrame) -> None:
    """A fragment that displays a DataFrame with a filter."""
    st.write(
        f"Processed {len(raw_files_df)} raw files. Latest update: {combined_df.iloc[0]['db_entry_created_at']}"
    )

    # filter
    filtered_df = show_filter(df, "Filter:")

    cmap = plt.get_cmap("RdYlGn")
    st.dataframe(
        filtered_df.style.background_gradient(
            subset="BasicStats_proteins_mean", cmap=cmap
        )
    )


display(combined_df)
