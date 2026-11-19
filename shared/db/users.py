"""User queries."""

import logging

from shared.db._checks import inactive_errors
from shared.db._mongo import users as _backend
from shared.domain.errors import InvalidInputError
from shared.domain.inputs import parse
from shared.domain.users import (
    NewUser,
    User,
    UserStatus,
    UserUpdate,
    normalize_email,
)


def find(*, include_inactive: bool = False) -> list[User]:
    """Return the users, ordered by initials."""
    return _backend.find(include_inactive=include_inactive)


def project_owner(raw_file_id: str) -> User | None:
    """Return the active owner of the project of a raw file, None if there is none."""
    return _backend.project_owner(raw_file_id)


def owner_errors(email: str | None) -> list[str]:
    """Return an error if the owner with the email is not active, nothing if no email is given.

    :param email: as it is stored, cf. `normalize_email`

    Raises:
        NotFoundError: if there is no such user.

    """
    if email is None:
        return []
    return inactive_errors(
        f"User '{email}'", _backend.get(email).status, UserStatus.ACTIVE
    )


def add(*, email: str, initials: str, messenger_id: str | None = None) -> User:
    """Add a new active user, cf. `NewUser`.

    Raises:
        InvalidInputError: if a field is invalid, or the email or the initials are taken.

    """
    new = parse(NewUser, email=email, initials=initials, messenger_id=messenger_id)
    errors = []
    if _backend.email_taken(new.email):
        errors.append(f"A user with email '{new.email}' already exists.")
    if _backend.initials_taken(new.initials):
        errors.append(_initials_taken_message(new.initials))
    if errors:
        raise InvalidInputError(errors)

    user = _backend.add(new)
    logging.info(f"Added user: {new!r}")
    return user


def update(email: str, *, initials: str, messenger_id: str | None) -> None:
    """Update a user, cf. `UserUpdate`; the email is immutable.

    :param messenger_id: None clears it

    Raises:
        NotFoundError: if there is no such user.
        InvalidInputError: if a field is invalid, or another user has the initials.

    """
    email = normalize_email(email)
    changes = parse(UserUpdate, initials=initials, messenger_id=messenger_id)
    if _backend.initials_taken(changes.initials, except_email=email):
        raise InvalidInputError([_initials_taken_message(changes.initials)])

    _backend.update(email, changes)
    logging.info(f"Updated user: {email=} {changes!r}")


def _initials_taken_message(initials: str) -> str:
    """Return the error message for initials that another user has."""
    return f"A user with initials '{initials}' already exists."


def set_status(email: str, status: str) -> None:
    """Set the status of a user, used for soft-deleting.

    Raises:
        NotFoundError: if there is no such user.
        InvalidInputError: if the status is unknown or the one the user has.

    """
    email = normalize_email(email)
    current = _backend.get(email).status
    if status == current or status not in (UserStatus.ACTIVE, UserStatus.INACTIVE):
        raise InvalidInputError(
            [f"Cannot set user '{email}' from '{current}' to '{status}'."]
        )
    _backend.set_status(email, status)
    logging.info(f"Set user status: {email=} {status=}")
