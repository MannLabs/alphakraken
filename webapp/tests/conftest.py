"""Shared code for all tests."""

import os

import pytest
import streamlit as st

os.environ["ENV_NAME"] = "_test_"
os.environ["MOUNTS_PATH"] = "./tmp/test/mounts"


@pytest.fixture(autouse=True)
def _clear_caches() -> None:
    """Keep the pages' cached DB reads from leaking between tests."""
    st.cache_data.clear()
