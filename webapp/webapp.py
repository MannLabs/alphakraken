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
    st.Page("pages_/home.py", title="Home", icon="🏠"),
    st.Page("pages_/overview.py", title="Overview", icon="📊"),
    st.Page("pages_/status.py", title="Status", icon="🚦"),
    st.Page("pages_/projects.py", title="Projects", icon="📁"),
    st.Page("pages_/settings.py", title="Project settings", icon="📋"),
]

# Create navigation
pg = st.navigation(pages, position="top")

# Run the selected page
pg.run()
