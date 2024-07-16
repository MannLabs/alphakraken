"""Simple status overview."""

import pandas as pd

# ruff: noqa: PD002 # `inplace=True` should be avoided; it has inconsistent behavior
import streamlit as st
from service.components import (
    display_status,
)
from service.data_handling import get_combined_raw_files_and_metrics_df
from service.db import df_from_db_data, get_status_data
from service.utils import _log

_log(f"loading {__file__}")

# ########################################### PAGE HEADER

st.set_page_config(page_title="AlphaKraken: status", layout="wide")
st.markdown("# Status")


# ########################################### DISPLAY: table & plot


@st.experimental_fragment
def _display_status(combined_df: pd.DataFrame) -> None:
    """A fragment that displays the status information."""
    st.markdown("## Status")
    try:
        status_data_df = df_from_db_data(get_status_data())

        status_data_df["updated_at_"] = status_data_df["updated_at_"].apply(
            lambda x: x.replace(microsecond=0)
        )
        display_status(combined_df, status_data_df)
        # show_status_plot(combined_df)
    except Exception as e:  # noqa: BLE001
        _log(str(e))
        st.warning(f"Cannot not display status: {e}.")


combined_df = get_combined_raw_files_and_metrics_df()

_display_status(combined_df)

c1, _ = st.columns([0.5, 0.5])
with c1.expander("Click here for help ..."):
    st.markdown("""
        ### Explanation
        - `last_file_creation`: timestamp of the youngest file that was picked up by the Kraken.

        - `last_status_update`: timestamp of the most recent update of a raw file status.

        - `last_file_check`: timestamp of the last check for new files. If this is > 5 minutes, something is wrong with
        the instrument_watcher DAG.

        """)
