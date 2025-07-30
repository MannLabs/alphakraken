"""Module wrapping access to Streamlit's query params."""

from typing import Any

import streamlit as st
from service.session_state import get_session_state


class QueryParams:
    """Keys for accessing query parameters."""

    # max age of data to load from the DB
    MAX_AGE = "max_age"

    # instruments to load from the DB
    INSTRUMENTS = "instruments"

    # max length of table to display
    MAX_TABLE_LEN = "max_table_len"

    # prefilled filter string
    FILTER = "filter"

    # whether page is accessed via mobile
    MOBILE = "mobile"

    # comma-separated list of baseline runs
    BASELINE = "baseline"


def set_query_param_from_session_state(
    key: str, query_param: str, default: str
) -> None:
    """Clear or set a query parameter from session state."""
    value = get_session_state(key)
    if value == default:
        if query_param in st.query_params:
            del st.query_params[query_param]
    else:
        st.query_params[query_param] = value


def get_all_query_params() -> dict[str, Any]:
    """Get all query parameters as a dictionary."""
    return st.query_params


def get_query_param(key: str, *, default: Any | None = None) -> Any:  # noqa: ANN401
    """Get a value from the query params, returning a default value if the key does not exist."""
    for k, v in st.query_params.items():
        if k.lower() == key.lower():
            return v
    return default


def is_query_param_true(key: str) -> bool:
    """Whether app is called with parameter `key` equal to 'True' (case-insensitive) or '1'."""
    return get_query_param(key, default="False").lower() in ["true", "1"]
