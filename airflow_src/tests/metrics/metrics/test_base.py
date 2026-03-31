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
    datastore = DataStore(Path("data_dir"), {"key": lambda _, **kwargs: "data"})  # noqa: ARG005

    # when
    result = datastore["key"]

    assert result == "data"


@patch("os.path.join")
def test_datastore_getitem_returns_data_when_key_in_data(mock_join: MagicMock) -> None:
    """Test that __getitem__ returns data when key in data store."""
    mock_join.return_value = "file_path"
    datastore = DataStore(Path("data_dir"), {"key": lambda _, **kwargs: "data"})  # noqa: ARG005
    datastore._data["key"] = "existing_data"

    # when
    result = datastore["key"]

    assert result == "existing_data"


def test_datastore_get_passes_columns_to_read_method() -> None:
    """Test that get() passes columns to the read method."""
    read_fn = MagicMock(return_value="filtered_data")
    datastore = DataStore(Path("data_dir"), {"key": read_fn})

    result = datastore.get("key", columns=["col_a", "col_b"])

    assert result == "filtered_data"
    read_fn.assert_called_once_with(
        Path("data_dir") / "key", columns=["col_a", "col_b"]
    )


def test_datastore_get_caches_by_columns() -> None:
    """Test that get() caches separately for different column sets."""
    call_count = 0

    def counting_read(_, **kwargs):  # noqa: ANN202, ANN001, ARG001
        nonlocal call_count
        call_count += 1
        return f"data_{call_count}"

    datastore = DataStore(Path("data_dir"), {"key": counting_read})

    result_ab = datastore.get("key", columns=["col_a", "col_b"])
    result_ab_again = datastore.get("key", columns=["col_b", "col_a"])
    result_c = datastore.get("key", columns=["col_c"])

    assert result_ab == "data_1"
    assert result_ab_again == "data_1"
    assert result_c == "data_2"
    assert call_count == 2
