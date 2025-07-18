"""Module wrapping access to Streamlit's session state."""

from typing import Any

import streamlit as st


class SessionStateKeys:
    """Keys for accessing session state."""

    SUCCESS_MSG = "success_msg"
    ERROR_MSG = "error_msg"

    CURRENT_FILTER = "current_filter"
    SHOW_TRACES = "show_traces"
    SHOW_STD = "show_std"
    SHOW_TRENDLINE = "show_trendline"
    PLOTS_PER_ROW = "plots_per_row"


def set_session_state(key: str, value: Any, *, overwrite: bool = True) -> None:  # noqa: ANN401
    """Set a value in the session state, optionally overwriting it if it already exists."""
    if overwrite or key not in st.session_state:
        st.session_state[key] = value


def get_session_state(key: str, *, default: Any | None = None) -> Any:  # noqa: ANN401
    """Get a value from the session state, returning a default value if the key does not exist."""
    return st.session_state.get(key, default)


def remove_session_state(key: str) -> None:
    """Remove a key from the session state."""
    del st.session_state[key]


def copy_session_state(target_key: str, source_key: str) -> None:
    """Copy a value from one key to another in the session state.

    Raises KeyError if the source key does not exist.
    """
    st.session_state[target_key] = st.session_state[source_key]
