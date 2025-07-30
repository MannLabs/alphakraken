"""Utilities for the streamlit app."""

import io
import logging
import os
import traceback
from pathlib import Path

import plotly.graph_objects as go
import streamlit as st
from PIL import Image
from service.query_params import QueryParams, is_query_param_true
from service.session_state import (
    SessionStateKeys,
    get_session_state,
    remove_session_state,
)

from shared.keys import Locations
from shared.yamlsettings import get_path

# mapping of filter strings to url query parameters
FILTER_MAPPING: dict[str, str] = {
    "_AND_": " & ",
    "_IS_": "=",
}

# TODO: remove this hack once https://github.com/streamlit/streamlit/issues/8112 is available
APP_URL = os.getenv("WEBAPP_URL")

DISABLE_WRITE = False


quanting_settings_path = get_path(Locations.SETTINGS)
quanting_output_path = get_path(Locations.OUTPUT)


DEFAULT_MAX_TABLE_LEN = 500
DEFAULT_MAX_AGE_OVERVIEW = 2  # days
DEFAULT_MAX_AGE_STATUS = 90  # days

BASELINE_PREFIX = "BASELINE_"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/webapp.log")
        if os.getenv("IS_PYTEST_RUN", "0") != "1"
        else None,
        logging.StreamHandler(),  # Keep console output for debugging
    ],
)
logger = logging.getLogger(__name__)


def _log(item_to_log: str | Exception, extra_msg: str = "") -> None:
    """Write a log message and show it if it's an exception."""
    if isinstance(item_to_log, Exception):
        if extra_msg:
            st.write(extra_msg)
            st.write(item_to_log)
        msg = f"{extra_msg}{item_to_log}\n{traceback.format_exc()}"
        logger.error(msg, exc_info=True)
    else:
        logger.info(item_to_log)


def empty_to_none(value: str) -> str | None:
    """Convert an empty string to None, pass through non-empty strings.

    Because streamlit returns "" for empty text inputs, we need to convert this to None
    to have the db schema to the validation of "required" fields correctly.
    """
    return None if value is None or value.strip() == "" else value


class Cols:
    """Internal column names."""

    IS_BASELINE = "is_baseline"


def show_feedback_in_sidebar() -> None:
    """Show any success or error messages in the sidebar."""
    for key in [SessionStateKeys.SUCCESS_MSG, SessionStateKeys.ERROR_MSG]:
        if msg := get_session_state(key):
            if key == SessionStateKeys.SUCCESS_MSG:
                msg_to_show = f"Success! {msg}"
                st.sidebar.success(msg_to_show)
            else:
                msg_to_show = f"Error! If you feel this is a bug, send a screenshot to the AlphaKraken team!\n\n{msg}"
                st.sidebar.error(msg_to_show)
            remove_session_state(key)


def display_info_message(st_display: st.delta_generator.DeltaGenerator = None) -> None:
    """Read an info message from a file and display it as a streamlit info message."""
    file_path = Path("/app/webapp/info_message.txt")
    if not file_path.exists():
        return

    with file_path.open() as f:
        content = f.read()
    if content:
        if st_display is None:
            c1, _ = st.columns([0.5, 0.5])
            c1.info(content, icon="ℹ️")  # noqa: RUF001
        else:
            st_display.info(content, icon="ℹ️")  # noqa: RUF001


def display_plotly_chart(
    fig: go.Figure, display: st.delta_generator.DeltaGenerator = st, **kwargs
) -> None:
    """Display a plotly chart in a streamlit app."""
    # currently, the mobile setup does not support plotly charts
    if is_query_param_true(QueryParams.MOBILE):
        img_bytes = fig.to_image(format="png", engine="kaleido")
        img = Image.open(io.BytesIO(img_bytes))

        display.image(img)
    else:
        display.plotly_chart(fig, **kwargs)
