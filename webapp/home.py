"""A very simple Streamlit app that connects to a MongoDB database and displays the data from the RawFile collection."""

import streamlit as st
from service.utils import _log

_log(f"loading {__file__}")

st.set_page_config(page_title="AlphaKraken: home", layout="wide")

"""
# Welcome to AlphaKraken!

Here you will soon find a bit of an explanation of how to use this.
For now, only the "overview" tab contains meaningful information.
"""
