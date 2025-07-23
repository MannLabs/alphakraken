"""Module to display status warnings in the Streamlit app."""

import streamlit as st
from service.db import df_from_db_data, get_status_data

from shared.db.models import KrakenStatusValues


# from service.db import df_from_db_data, get_status_data
def display_status_warning() -> None:
    """Display a warning if there are instruments with issues."""
    status_data_df = df_from_db_data(get_status_data())
    nok_status_df = status_data_df[status_data_df["status"] != KrakenStatusValues.OK]
    if len(nok_status_df):
        instruments_with_issues = nok_status_df["_id"].unique()
        st.warning(
            f"AlphaKraken currently has issues monitoring the following instrument(s): **{', '.join(instruments_with_issues)}** . "
            "Please check the status page for more information.",
            "Don't worry: the instrument is quite likely working fine.",
            icon="⚠️",
        )
        st.page_link("pages_/status.py", label="➔ Go to status page")
