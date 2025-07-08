"""Main AlphaKraken webapp with navigation."""

import streamlit as st
from service.utils import _log

_log(f"loading {__file__}")


# Set page config
st.set_page_config(
    page_title="AlphaKraken",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Define pages
pages = [
    st.Page("pages_/home.py", title="🏠 Home"),
    st.Page("pages_/overview.py", title="📈  Overview"),
    st.Page("pages_/status.py", title="📊 Status"),
    st.Page("pages_/projects.py", title="📁 Projects"),
    st.Page("pages_/settings.py", title="📋 Project settings"),
]

# Create navigation
pg = st.navigation(pages, position="top")

# Run the selected page
pg.run()
