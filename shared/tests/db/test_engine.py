"""Tests for the db.engine module."""

from unittest.mock import MagicMock, patch

from mongoengine import ConnectionFailure

from shared.db.engine import connect_db


@patch("shared.db.engine.connect")
def test_connect_db_successful_connection(mock_connect: MagicMock) -> None:
    """Test that connect_db successfully connects to the database."""
    # when
    connect_db()

    # then
    mock_connect.assert_called_once()


@patch("shared.db.engine.connect", side_effect=ConnectionFailure)
def test_connect_db_connection_failure(mock_connect: MagicMock) -> None:
    """Test that connect_db handles a connection failure gracefully."""
    # when
    connect_db()

    # then
    mock_connect.assert_called_once()
