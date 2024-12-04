"""Utilities for the streamlit app."""

import os
import traceback
from datetime import datetime
from pathlib import Path

import pytz
import streamlit as st

# mapping of filter strings to url query parameters
FILTER_MAPPING: dict[str, str] = {
    "_AND_": " & ",
    "_IS_": "=",
}

# TODO: remove this hack once https://github.com/streamlit/streamlit/issues/8112 is available
APP_URL = "http://<kraken_url>"


class QueryParams:
    """Query parameters for streamlit pages."""

    # max age of data to load from the DB
    MAX_AGE = "max_age"

    # max length of table to display
    MAX_TABLE_LEN = "max_table_len"

    # prefilled filter string
    FILTER = "filter"


DEFAULT_MAX_AGE_OVERVIEW = 30  # days
DEFAULT_MAX_AGE_STATUS = 7  # days
DEFAULT_MAX_TABLE_LEN = 500


def _log(item_to_log: str | Exception, extra_msg: str = "") -> None:
    """Write a log message and show it if it's an exception."""
    now = datetime.now(tz=pytz.UTC).strftime("%Y-%m-%d %H:%M:%S.%f")

    if isinstance(item_to_log, Exception):
        if extra_msg:
            st.write(extra_msg)
        st.write(item_to_log)
        msg = f"{extra_msg}{item_to_log}\n{traceback.format_exc()}"
    else:
        msg = f"{now}: {item_to_log}"

    os.write(1, f"{msg}\n".encode())


def empty_to_none(value: str) -> str | None:
    """Convert an empty string to None, pass through non-empty strings.

    Because streamlit returns "" for empty text inputs, we need to convert this to None
    to have the db schema to the validation of "required" fields correctly.
    """
    return None if value is None or value.strip() == "" else value


class SessionStateKeys:
    """Keys for the session state."""

    SUCCESS_MSG = "success_msg"
    ERROR_MSG = "error_msg"


def show_feedback_in_sidebar() -> None:
    """Show any success or error messages in the sidebar."""
    for key in [SessionStateKeys.SUCCESS_MSG, SessionStateKeys.ERROR_MSG]:
        if msg := st.session_state.get(key, False):
            if key == SessionStateKeys.SUCCESS_MSG:
                msg_to_show = f"Success! {msg}"
                st.sidebar.success(msg_to_show)
            else:
                msg_to_show = f"Error! If you feel this is a bug, send a screenshot to the AlphaKraken team!\n\n{msg}"
                st.sidebar.error(msg_to_show)
            del st.session_state[key]


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
