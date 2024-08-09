"""A very simple Streamlit app that connects to a MongoDB database and displays the data from the RawFile collection."""

import streamlit as st
from service.utils import _log

_log(f"loading {__file__}")

st.set_page_config(page_title="AlphaKraken: home", layout="wide")

"""
# Welcome to AlphaKraken!
Note: you are currently viewing the 'sandbox' environment. Everything could
change any minute, and data could be gone at any time.
Also, bear in mind that the whole AlphaKraken project is currenly work in progress.

Still, feedback on the current state is always welcome!


## A basic explanation of how to use this:

If you want to see data for all raw files that have been processed, go to the "overview" tab.

To create a new project, go to the "projects" tab.
"""
