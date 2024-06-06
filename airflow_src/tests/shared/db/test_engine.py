"""Tests for the db.engine module."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytz
from mongoengine import ConnectionFailure
from plugins.common.settings import RawFileStatus

from shared.db.engine import (
    add_new_raw_file_to_db,
    connect_db,
    get_raw_file_names_from_db,
)


@patch("shared.db.engine.disconnect")
@patch("shared.db.engine.connect")
def test_connect_db_successful_connection(
    mock_connect: MagicMock, mock_disconnect: MagicMock
) -> None:
    """Test that connect_db successfully connects to the database."""
    # when
    connect_db()

    # then
    mock_disconnect.assert_called_once()
    mock_connect.assert_called_once()


@patch("shared.db.engine.disconnect")
@patch("shared.db.engine.connect", side_effect=ConnectionFailure)
def test_connect_db_connection_failure(
    mock_connect: MagicMock, mock_disconnect: MagicMock
) -> None:
    """Test that connect_db handles a connection failure gracefully."""
    # when
    connect_db()

    # then
    mock_disconnect.assert_called_once()
    mock_connect.assert_called_once()


@patch("shared.db.engine.connect_db")
@patch("shared.db.engine.RawFile")
def test_add_new_raw_file_to_db_creates_new_file_when_file_does_not_exist(
    mock_raw_file: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that add_new_raw_file_to_db creates a new file when the file does not exist in the database."""
    # given
    mock_raw_file.return_value.save.side_effect = None
    # when
    add_new_raw_file_to_db(
        "test_file.raw", instrument_id="instrument1", size=42.0, creation_ts=43.0
    )

    # then
    mock_raw_file.assert_called_once_with(
        name="test_file.raw",
        status=RawFileStatus.NEW,
        size=42.0,
        instrument_id="instrument1",
        created_at=datetime(1970, 1, 1, 0, 0, 43, tzinfo=pytz.utc),
    )
    mock_connect_db.assert_called_once()


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
