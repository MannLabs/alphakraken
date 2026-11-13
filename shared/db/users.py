"""User queries."""

import logging
from typing import Any

from shared.db._checks import check_status_transition
from shared.db._mongo import users as _backend
from shared.domain.errors import ValidationError
from shared.domain.inputs import parse
from shared.domain.users import (
    USER_STATUS_TRANSITIONS,
    NewUser,
    User,
    UserStatus,
    UserUpdate,
)


def find(*, include_inactive: bool = False) -> list[User]:
    """Return the users, ordered by initials."""
    return _backend.find(include_inactive=include_inactive)


def project_owner(raw_file_id: str) -> User | None:
    """Return the active owner of the project of a raw file, None if there is none."""
    return _backend.project_owner(raw_file_id)


def check_active(email: str) -> list[str]:
    """Return an error message if the user with the email is inactive, else nothing.

    Raises:
        NotFoundError: if there is no such user.

    """
    if _backend.get(email).status != UserStatus.ACTIVE:
        return [f"User '{email}' is inactive."]
    return []


def add(**fields: Any) -> User:
    """Add a new active user.

    :param fields: the fields of `NewUser`

    Raises:
        ValidationError: if a field is missing or invalid, or the email or the initials are taken.

    """
    new = parse(NewUser, **fields)
    errors = []
    if _backend.email_taken(new.email):
        errors.append(f"A user with email '{new.email}' already exists.")
    if _backend.initials_taken(new.initials):
        errors.append(_initials_taken_message(new.initials))
    if errors:
        raise ValidationError(errors)

    user = _backend.add(new)
    logging.info(f"Added user: {new!r}")
    return user


def update(email: str, **fields: Any) -> None:
    """Update a user, the email is immutable.

    :param fields: the fields of `UserUpdate`

    Raises:
        NotFoundError: if there is no such user.
        ValidationError: if a field is missing or invalid, or another user has the initials.

    """
    changes = parse(UserUpdate, **fields)
    if _backend.initials_taken(changes.initials, except_email=email):
        raise ValidationError([_initials_taken_message(changes.initials)])

    _backend.update(email, changes)
    logging.info(f"Updated user: {email=} {changes!r}")


def _initials_taken_message(initials: str) -> str:
    return f"A user with initials '{initials}' already exists."


def set_status(email: str, status: str) -> None:
    """Set the status of a user, used for soft-deleting.

    Raises:
        NotFoundError: if there is no such user.
        ValidationError: if the status is unknown or the one the user has.

    """
    check_status_transition(
        USER_STATUS_TRANSITIONS, f"user '{email}'", _backend.get(email).status, status
    )
    _backend.set_status(email, status)
    logging.info(f"Set user status: {email=} {status=}")
