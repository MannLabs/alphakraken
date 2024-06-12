"""A very simple Streamlit app that connects to a MongoDB database and displays the data from the RawFile collection."""
# ruff: noqa: PD002 inplace=True should be avoided

from collections import defaultdict

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st

from shared.db.engine import Metrics, RawFile, connect_db

"""
# Welcome to AlphaKraken!
"""

connect_db()

st.write(f"Processed {RawFile.objects.count()} raw files.")

# TODO: this throwaway code!

# get raw file data
raw_file_data = defaultdict(list)
for raw_file in RawFile.objects.order_by("-created_at"):
    raw_file_data["name"].append(raw_file.name)
    raw_file_data["size_gb"].append(raw_file.size / 1024**3)
    raw_file_data["status"].append(raw_file.status)
    raw_file_data["created_at"].append(raw_file.created_at)
    raw_file_data["instrument_id"].append(raw_file.instrument_id)

raw_file_data_df = pd.DataFrame(raw_file_data)

# get metrics
metrics_data = defaultdict(list)
for metric in Metrics.objects().order_by("-created_at"):
    for k, v in metric.to_mongo().items():
        metrics_data[k].append(v)

metrics_data_df = pd.DataFrame(metrics_data)
metrics_data_df.drop_duplicates(subset=["raw_file"], keep="last", inplace=True)
metrics_data_df.drop(
    columns=[
        "_id",
        "db_entry_created_at",
    ],
    inplace=True,
    errors="ignore",
)
metrics_data_df.rename(columns={"raw_file": "name"}, inplace=True, errors="ignore")

# merge
try:
    to_show_df = raw_file_data_df.merge(metrics_data_df, on="name", how="left")
except Exception:  # noqa: BLE001
    to_show_df = raw_file_data_df


def show_filter(to_show_df: pd.DataFrame, text: str, column: str) -> pd.DataFrame:
    """Filter the DataFrame based on user input."""
    user_input = st.text_input(text, None)
    if user_input is not None:
        return to_show_df[to_show_df[column].str.contains(user_input)]
    return to_show_df


to_show_df2 = show_filter(to_show_df, "Raw file name", "name")
to_show_df3 = show_filter(to_show_df2, "Instrument", "name")


cmap = plt.get_cmap("RdYlGn")

my_table = st.dataframe(
    to_show_df3.style.background_gradient(subset="BasicStats_proteins_mean", cmap=cmap)
)
