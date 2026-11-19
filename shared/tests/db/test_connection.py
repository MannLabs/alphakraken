"""Tests for the connection and the error translation of the Mongo backend."""

from unittest.mock import MagicMock, patch

import bson.errors
import mongoengine
import pymongo.errors
import pytest
from mongoengine import ConnectionFailure

from shared import db
from shared.db._mongo import models
from shared.db._mongo.connection import (
    connect_db,
    db_backend_function,
    ensure_connected,
)
from shared.db._mongo.errors import translate_errors
from shared.domain.errors import (
    AlreadyExistsError,
    DatabaseError,
    DatabaseUnavailableError,
    InvalidInputError,
    NotFoundError,
)


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


# error translation


@pytest.mark.parametrize(
    ("raised", "expected"),
    [
        (mongoengine.DoesNotExist("gone"), NotFoundError),
        (mongoengine.NotUniqueError("twice"), AlreadyExistsError),
        (pymongo.errors.DuplicateKeyError("twice"), AlreadyExistsError),
        (pymongo.errors.ServerSelectionTimeoutError("down"), DatabaseUnavailableError),
        (pymongo.errors.OperationFailure("not authorized"), DatabaseError),
        (mongoengine.ConnectionFailure("alias registered"), DatabaseError),
        (mongoengine.OperationError("not authorized"), DatabaseError),
        (mongoengine.MultipleObjectsReturned("two"), DatabaseError),
        (mongoengine.InvalidQueryError("bad query"), DatabaseError),
        (bson.errors.InvalidDocument("cannot encode object"), InvalidInputError),
    ],
)
def test_translate_errors(raised: Exception, expected: type[Exception]) -> None:
    """Provider exceptions become domain errors with the original message."""

    @translate_errors
    def fail() -> None:
        raise raised

    with pytest.raises(expected, match=str(raised)):
        fail()


def test_translate_errors_lists_one_message_per_invalid_field() -> None:
    """A mongoengine validation error carries one message per field."""
    document = models.User(email="no-at-sign", initials="1")

    @translate_errors
    def fail() -> None:
        document.validate()

    with pytest.raises(InvalidInputError) as excinfo:
        fail()
    assert sorted(message.split(":")[0] for message in excinfo.value.errors) == [
        "email",
        "initials",
    ]


def test_translate_errors_passes_results_and_other_exceptions_through() -> None:
    """Unrelated exceptions are not translated."""

    @translate_errors
    def compute(x: int) -> int:
        if x < 0:
            raise ValueError("negative")
        return x * 2

    assert compute(2) == 4
    with pytest.raises(ValueError, match="negative"):
        compute(-1)


# connection


@pytest.fixture(autouse=True)
def _forget_connection() -> None:
    ensure_connected.cache_clear()


@patch("shared.db._mongo.connection.connect_db")
def test_ensure_connected_registers_once(mock_connect_db: MagicMock) -> None:
    """A successful registration is not repeated."""
    ensure_connected()
    ensure_connected()

    mock_connect_db.assert_called_once_with(raise_on_error=True)


@patch("shared.db._mongo.connection.connect_db")
def test_ensure_connected_retries_after_failure(mock_connect_db: MagicMock) -> None:
    """A failed registration is retried on the next call."""
    mock_connect_db.side_effect = [mongoengine.ConnectionFailure("boom"), None]

    with pytest.raises(mongoengine.ConnectionFailure):
        ensure_connected()
    ensure_connected()

    assert mock_connect_db.call_count == 2


@patch("shared.db._mongo.connection.get_db")
@patch("shared.db._mongo.connection.connect_db")
def test_ping(mock_connect_db: MagicMock, mock_get_db: MagicMock) -> None:
    """A ping connects and asks the server."""
    db.ping()

    mock_connect_db.assert_called_once()
    mock_get_db.return_value.command.assert_called_once_with("ping")


@patch("shared.db._mongo.connection.get_db")
@patch("shared.db._mongo.connection.connect_db")
def test_ping_raises_when_database_is_unreachable(
    mock_connect_db: MagicMock,  # noqa: ARG001
    mock_get_db: MagicMock,
) -> None:
    """An unreachable server is reported as the domain error clients catch."""
    mock_get_db.return_value.command.side_effect = (
        pymongo.errors.ServerSelectionTimeoutError("down")
    )

    with pytest.raises(DatabaseUnavailableError, match="down"):
        db.ping()
