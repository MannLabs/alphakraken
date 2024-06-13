"""Utilities for the streamlit app."""

import os


def _log(msg: str) -> None:
    """Write a log message."""
    os.write(1, f"{msg}\n".encode())
