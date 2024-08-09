"""A very simple Streamlit app that connects to a MongoDB database and displays the data from the RawFile collection."""

import streamlit as st
from shared.db.engine import RawFile, connect_db

"""
# Welcome to AlphaKraken!
Here's your data:
"""

connect_db()

st.write(f"Found {RawFile.objects.count()} raw files.")

for raw_file in RawFile.objects.order_by("-created_at"):
    st.write(f"{raw_file.name} : {raw_file.status} [{raw_file.created_at}]")
