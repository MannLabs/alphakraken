"""A very simple Streamlit app that connects to a MongoDB database and displays the data from the RawFile collection."""

import os

import streamlit as st
from service.components import show_sandbox_message
from service.utils import _log

from shared.keys import EnvVars

_log(f"loading {__file__}")

st.set_page_config(page_title="AlphaKraken: home", layout="wide")

"""
# Welcome to AlphaKraken!
"""

if os.environ.get(EnvVars.ENV_NAME) == "production":
    st.warning("""

    Note: you are currently viewing the very first version of the AlphaKraken.
    Bear in mind that the project is far from complete in terms of features.

    If you are missing something or have a cool idea or found a bug, please let us know: <support_email>
    """)
else:
    show_sandbox_message()

"""
## A basic explanation of how to use this:

If you want to see data for all raw files that have been processed, go to the "overview" tab.

That's it, the rest is only for admin users.
"""
