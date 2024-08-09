"""A very simple Streamlit app that connects to a MongoDB database and displays the data from the RawFile collection."""

from collections import defaultdict

import pandas as pd
import streamlit as st

from shared.db.engine import RawFile, connect_db

"""
# Welcome to AlphaKraken!
Here's your data:
"""

connect_db()

st.write(f"Processed {RawFile.objects.count()} raw files.")


raw_file_data = defaultdict(list)
for raw_file in RawFile.objects.order_by("-created_at"):
    raw_file_data["name"].append(raw_file.name)
    raw_file_data["size"].append(raw_file.size)
    raw_file_data["status"].append(raw_file.status)
    raw_file_data["created_at"].append(raw_file.created_at)
    raw_file_data["instrument_id"].append(raw_file.instrument_id)

raw_file_data_df = pd.DataFrame(raw_file_data)
my_table = st.dataframe(raw_file_data_df)
