"""Main AlphaKraken webapp with navigation."""

import os

import streamlit as st
from service.admin import is_admin, show_admin_login
from service.utils import _log

from shared.keys import EnvVars

_log(f"loading {__file__}")


page_icon = (
    "webapp/resources/alphakraken.png"
    if os.environ.get(EnvVars.ENV_NAME) == "production"
    else "🦑"
)

# Set page config
st.set_page_config(
    page_title="AlphaKraken",
    layout="wide",
    initial_sidebar_state="collapsed",
    page_icon=page_icon,
)

# Define pages
pages = [
    st.Page("pages_/home.py", title="🏠 Home"),
    st.Page("pages_/overview.py", title="📈  Overview"),
    st.Page("pages_/status.py", title="📊 Status"),
    st.Page("pages_/projects.py", title="📁 Manage projects"),
    st.Page("pages_/settings.py", title="📋 Manage settings"),
]
# non-admins can't reach pages that are not registered
if is_admin():
    pages.append(st.Page("pages_/admin.py", title="🔧 Admin"))

# Create navigation
pg = st.navigation(pages, position="top")

show_admin_login()

# Run the selected page
pg.run()
