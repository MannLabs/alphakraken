"""Test that all pages render on a fresh deployment, i.e. with an empty database."""

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import streamlit as st
from streamlit.testing.v1 import AppTest

PAGES_FOLDER = Path(__file__).parent / Path("../../pages_")

# home.py is left out: it reads no data from the database
PAGES = ["overview.py", "status.py", "projects.py", "settings.py"]

# generous, as the first page run also pays the matplotlib font cache build
RUN_TIMEOUT_SECONDS = 30

DB_MODELS = ["RawFile", "Metrics", "KrakenStatus", "Project", "Settings"]


def _empty_query_set() -> MagicMock:
    """Get a mock QuerySet that yields no documents, whatever is chained on it."""
    query_set = MagicMock()
    query_set.__iter__ = MagicMock(side_effect=lambda: iter([]))
    query_set.__len__ = MagicMock(return_value=0)
    query_set.__bool__ = MagicMock(return_value=False)
    for method in ["only", "exclude", "order_by", "filter"]:
        getattr(query_set, method).return_value = query_set
    query_set.return_value = query_set
    return query_set


@pytest.fixture
def _empty_db() -> Iterator[None]:
    """Make every database collection appear empty."""
    with (
        patch("shared.db.interface.connect_db"),
        patch("service.db.connect_db"),
        patch.multiple(
            "service.db", **{model: MagicMock() for model in DB_MODELS}
        ) as mocks,
    ):
        for mock in mocks.values():
            mock.objects = _empty_query_set()
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
