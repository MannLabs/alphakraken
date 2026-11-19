"""Test that all pages render on a fresh deployment, i.e. with an empty database."""

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

PAGES_FOLDER = Path(__file__).parent / Path("../../pages_")

# home.py is left out: it reads no data from the database
PAGES = ["overview.py", "status.py", "projects.py", "settings.py"]

# generous, as the first page run also pays the matplotlib font cache build
RUN_TIMEOUT_SECONDS = 30


@pytest.fixture
def _empty_db() -> Iterator[None]:
    """Make every database query return nothing."""
    with (
        patch("shared.db.raw_files.find", return_value=[]),
        patch("shared.db.metrics.latest_by_raw_file", return_value={}),
        patch("shared.db.status.find", return_value=[]),
        patch("shared.db.settings.find", return_value=[]),
        patch("shared.db.projects.find", return_value=[]),
        patch("shared.db.project_settings.find", return_value=[]),
        patch("shared.db.users.find", return_value=[]),
    ):
        yield


@pytest.fixture(autouse=True)
def _clear_streamlit_cache() -> Iterator[None]:
    """Keep the empty-DB data out of the cache that the pages share with other tests."""
    st.cache_data.clear()
    yield
    st.cache_data.clear()


@pytest.mark.usefixtures("_empty_db")
@pytest.mark.parametrize("page", PAGES)
def test_page_renders_on_empty_db(page: str) -> None:
    """Test that a page renders without exception when no document exists in any collection."""
    at = AppTest.from_file(f"{PAGES_FOLDER}/{page}").run(timeout=RUN_TIMEOUT_SECONDS)

    assert not at.exception
