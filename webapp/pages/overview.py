"""Simple data overview."""
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

# the joining could also be done on DB level
raw_file_and_metrics_df = raw_files_df.merge(
    metrics_df, left_on="_id", right_on="raw_file", how="left"
)

filtered_df = show_filter(raw_file_and_metrics_df, "Filter:")

filtered_df.sort_values(by="created_at", ascending=False, inplace=True)
filtered_df.reset_index(drop=True, inplace=True)


# ########################################### DISPLAY

st.write(
    f"Processed {raw_files_db.count()} raw files. Latest update: {filtered_df.iloc[0]['db_entry_created_at']}"
)

cmap = plt.get_cmap("RdYlGn")

my_table = st.dataframe(
    filtered_df.style.background_gradient(subset="BasicStats_proteins_mean", cmap=cmap)
)
