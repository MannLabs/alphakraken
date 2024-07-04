"""Utilities for the streamlit app."""

import os
from datetime import datetime

import pytz
import streamlit as st


def _log(msg: str) -> None:
    """Write a log message."""
    now = datetime.now(tz=pytz.UTC).strftime("%Y-%m-%d %H:%M:%S.%f")
    os.write(1, f"{now}: {msg}\n".encode())


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
