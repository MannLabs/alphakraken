"""Errors raised by `shared.db`, the only ones clients catch."""


class NotFoundError(Exception):
    """No record with the given id."""


class AlreadyExistsError(Exception):
    """A record with the same unique key exists."""


class ValidationError(Exception):
    """A record violates its constraints, one message per violation."""

    def __init__(self, errors: list[str]) -> None:
        """Initialize with the violation messages."""
        super().__init__("; ".join(errors))
        self.errors = errors


class DatabaseError(Exception):
    """Any database failure not covered by a more specific error."""


class DatabaseUnavailableError(DatabaseError):
    """The database cannot be reached."""
