"""Shared fixtures for the page tests."""

from collections.abc import Iterator
from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def _no_users() -> Iterator[None]:
    """Run the pages without users by default, tests needing users patch on top."""
    with patch("service.owner.get_all_users", return_value=[]):
        yield
