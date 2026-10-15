"""Admin page, only registered in the navigation for admins."""

import streamlit as st
from service.query_params import get_all_query_params
from service.utils import _log

_log(f"loading {__file__} {get_all_query_params()}")

st.set_page_config(page_title="AlphaKraken: admin", layout="wide")

st.markdown("# Admin")

st.write("Nothing here yet.")
