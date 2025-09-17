"""Module to display status warnings in the Streamlit app."""

import streamlit as st
from service.db import df_from_db_data, get_status_data

from shared.db.models import KrakenStatusEntities, KrakenStatusValues


def display_status_warning() -> None:
    """Display a warning if there are instruments with issues."""
    status_data_df = df_from_db_data(get_status_data())
    nok_status_df = status_data_df[
        (status_data_df["entity_type"] == KrakenStatusEntities.INSTRUMENT)
        & (status_data_df["status"] != KrakenStatusValues.OK)
    ]
    if len(nok_status_df):
        instruments_with_issues = nok_status_df["_id"].unique()
        st.warning(
            f"Currently, monitoring of the following instrument(s) is disrupted: **{', '.join(instruments_with_issues)}** . "
            "Please check the status page for more information. "
            "Don't worry: the respective instruments are most likely working fine.",
            icon="⚠️",
        )
        st.page_link("pages_/status.py", label="➔ Go to status page")
