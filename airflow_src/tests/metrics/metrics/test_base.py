"""Tests for the metrics calculator."""

# ruff: noqa: PLR2004 # magic numbers are fine in tests
from pathlib import Path
from unittest.mock import MagicMock, patch

from plugins.metrics.metrics.base import DataStore


@patch("os.path.join")
def test_datastore_getitem_loads_data_when_key_not_in_data(
    mock_join: MagicMock,
) -> None:
    """Test that __getitem__ loads data when key not in data store."""
    mock_join.return_value = "file_path"
    datastore = DataStore(Path("data_dir"), {"key": lambda _: "data"})

    # when
    result = datastore["key"]

    assert result == "data"


@patch("os.path.join")
def test_datastore_getitem_returns_data_when_key_in_data(mock_join: MagicMock) -> None:
    """Test that __getitem__ returns data when key in data store."""
    mock_join.return_value = "file_path"
    datastore = DataStore(Path("data_dir"), {"key": lambda _: "data"})
    datastore._data["key"] = "existing_data"  # noqa: SLF001

    # when
    result = datastore["key"]

    assert result == "existing_data"
