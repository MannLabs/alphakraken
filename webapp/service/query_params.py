"""Module wrapping access to Streamlit's query params."""

from typing import Any

import streamlit as st


class QueryParamKeys:
    """Keys for accessing query parameters."""

    # Add common query parameter keys here as needed


def set_query_param(key: str, value: Any, *, overwrite: bool = True) -> None:  # noqa: ANN401
    """Set a value in the query params, optionally overwriting it if it already exists."""
    if overwrite or key not in st.query_params:
        st.query_params[key] = value


def get_query_param(key: str, *, default: Any | None = None) -> Any:  # noqa: ANN401
    """Get a value from the query params, returning a default value if the key does not exist."""
    return st.query_params.get(key, default)


def remove_query_param(key: str) -> None:
    """Remove a key from the query params."""
    del st.query_params[key]


def copy_query_param(target_key: str, source_key: str) -> None:
    """Copy a value from one key to another in the query params.

    Raises KeyError if the source key does not exist.
    """
    st.query_params[target_key] = st.query_params[source_key]
