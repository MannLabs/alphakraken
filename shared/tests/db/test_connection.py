"""Tests for the connection module of the Mongo backend."""

from unittest.mock import MagicMock, patch

import pymongo.errors
import pytest
from mongoengine import ConnectionFailure

from shared.db._mongo.connection import connect_db, db_backend_function
from shared.domain.errors import DatabaseError, DatabaseUnavailableError


@patch("shared.db._mongo.connection.connect")
def test_connect_db_successful_connection(mock_connect: MagicMock) -> None:
    """Test that connect_db successfully connects to the database."""
    # when
    connect_db()

    # then
    mock_connect.assert_called_once()


@patch("shared.db._mongo.connection.connect", side_effect=ConnectionFailure)
def test_connect_db_connection_failure(mock_connect: MagicMock) -> None:
    """Test that connect_db handles a connection failure gracefully."""
    # when
    connect_db()

    # then
    mock_connect.assert_called_once()


@patch("shared.db._mongo.connection.ensure_connected")
def test_backend_function_connects_before_the_call(
    mock_ensure_connected: MagicMock,
) -> None:
    """A backend function connects on every call and returns the result of the wrapped function."""

    @db_backend_function
    def double(x: int) -> int:
        mock_ensure_connected.assert_called_once()
        return x * 2

    assert double(2) == 4


@patch("shared.db._mongo.connection.ensure_connected")
def test_backend_function_translates_errors_of_connecting_and_of_the_call(
    mock_ensure_connected: MagicMock,
) -> None:
    """Provider errors become domain errors, whether raised while connecting or by the function."""

    @db_backend_function
    def fail() -> None:
        raise pymongo.errors.ServerSelectionTimeoutError("down")

    with pytest.raises(DatabaseUnavailableError, match="down"):
        fail()

    mock_ensure_connected.side_effect = ConnectionFailure("alias registered")
    with pytest.raises(DatabaseError, match="alias registered"):
        fail()
