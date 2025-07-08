"""Simple status overview."""

import pandas as pd

# ruff: noqa: PD002 # `inplace=True` should be avoided; it has inconsistent behavior
import streamlit as st
from service.components import (
    display_status,
    show_sandbox_message,
    show_status_plot,
    show_time_in_status_table,
)
from service.db import df_from_db_data, get_raw_files_for_status_df, get_status_data
from service.utils import DEFAULT_MAX_AGE_STATUS, QueryParams, _log

_log(f"loading {__file__}")

# ########################################### PAGE HEADER

st.set_page_config(page_title="AlphaKraken: status", layout="wide")

show_sandbox_message()

st.markdown("# Status")


# ########################################### DISPLAY: table & plot


@st.fragment
def _display_status(combined_df: pd.DataFrame) -> None:
    """A fragment that displays the status information."""
    try:
        status_data_df = df_from_db_data(get_status_data())

        if not len(status_data_df):
            st.warning("Not enough data yet.")
            return

        status_data_df["updated_at_"] = status_data_df["updated_at_"].apply(
            lambda x: x.replace(microsecond=0)
        )
        st.markdown("## Latest data")
        display_status(combined_df, status_data_df)

        c1, _ = st.columns([0.5, 0.5])
        with c1.expander("Click here for help ..."):
            st.info(
                """
                ### Explanation
                - `last_file_creation`: timestamp of the youngest file that was picked up by the Kraken.
                - `last_status_update`: timestamp of the most recent update of a raw file status.
                - `last_health_check`: timestamp of the last health check during check for new files. If this is > 10 minutes, something is wrong with
                                       the instrument_watcher DAG.
                """,
                icon="ℹ️",  # noqa: RUF001
            )

        st.markdown("## Current activity")

        c1, c2 = st.columns([0.5, 0.5])
        c1.markdown("### Distribution of statuses")
        show_status_plot(combined_df, c1)

        c2.markdown("### Oldest transition to status")
        show_time_in_status_table(combined_df, c2)

    except Exception as e:  # noqa: BLE001
        _log(e, "Cannot not display status information.")


combined_df = get_raw_files_for_status_df(
    # restricting the data retrieval also for the status page could in principle lead to some instruments not
    # being displayed anymore (after a long standstill), but this is a rare case:
    int(st.query_params.get(QueryParams.MAX_AGE, DEFAULT_MAX_AGE_STATUS))
)

_display_status(combined_df)
