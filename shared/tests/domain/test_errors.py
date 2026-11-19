"""Tests for the domain.errors module."""

from shared.domain.errors import (
    DatabaseError,
    DatabaseUnavailableError,
    InvalidInputError,
)


def test_validation_error_joins_messages() -> None:
    """The message lists all violations, which stay accessible individually."""
    error = InvalidInputError(["name is required", "version must be positive"])

    assert str(error) == "name is required; version must be positive"
    assert error.errors == ["name is required", "version must be positive"]


def test_database_unavailable_is_a_database_error() -> None:
    """Catching DatabaseError also catches an unavailable database."""
    assert issubclass(DatabaseUnavailableError, DatabaseError)
