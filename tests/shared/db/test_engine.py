"""Tests for the db.engine module."""

from unittest.mock import MagicMock, patch

from shared.db.engine import get_raw_file_names_from_db


@patch("shared.db.engine.connect_db")
@patch("shared.db.engine.RawFile")
def test_get_raw_file_names_from_db_returns_expected_names_when_files_exist(
    mock_raw_file: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that get_raw_file_names_from_db returns the expected names when the files exist in the database."""
    # given
    mock1 = MagicMock()
    mock1.name = "file1"
    mock2 = MagicMock()
    mock2.name = "file2"
    mock_raw_file.objects.filter.return_value = [mock1, mock2]

    # when
    result = get_raw_file_names_from_db(["file1", "file2"])

    # then
    assert result == ["file1", "file2"]
    mock_connect_db.assert_called_once()


@patch("shared.db.engine.connect_db")
@patch("shared.db.engine.RawFile")
def test_get_raw_file_names_from_db_returns_empty_list_when_no_files_exist(
    mock_raw_file: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that get_raw_file_names_from_db returns an empty list when no files exist in the database."""
    # given
    mock_raw_file.objects.filter.return_value = []
    # when
    result = get_raw_file_names_from_db(["file1", "file2"])
    # then
    assert result == []
    mock_connect_db.assert_called_once()


@patch("shared.db.engine.connect_db")
@patch("shared.db.engine.RawFile")
def test_get_raw_file_names_from_db_returns_only_existing_files_when_some_files_do_not_exist(
    mock_raw_file: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that get_raw_file_names_from_db returns only the names of the files that exist in the database."""
    # given
    mock1 = MagicMock()
    mock1.name = "file1"
    mock_raw_file.objects.filter.return_value = [mock1]
    # when
    result = get_raw_file_names_from_db(["file1", "file2"])
    # then
    assert result == ["file1"]
    mock_connect_db.assert_called_once()
