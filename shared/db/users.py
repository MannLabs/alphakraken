"""User queries."""

import logging

from shared.db._mongo import users as _backend
from shared.domain.users import User


def find(*, include_inactive: bool = False) -> list[User]:
    """Return the users, ordered by initials."""
    return _backend.find(include_inactive=include_inactive)


def project_owner(raw_file_id: str) -> User | None:
    """Return the active owner of the project of a raw file, None if there is none."""
    return _backend.project_owner(raw_file_id)


def add(*, email: str, initials: str, messenger_id: str | None = None) -> User:
    """Add a new active user, the email is normalized to lower case.

    Raises:
        ValidationError: if the email or the initials are taken.

    """
    user = _backend.add(email=email, initials=initials, messenger_id=messenger_id)
    logging.info(f"Added user: {user.email=} {initials=} {messenger_id=}")
    return user


def update(email: str, *, initials: str, messenger_id: str | None) -> None:
    """Update the initials and messenger id of a user, the email is immutable.

    Raises:
        ValidationError: if another user has the initials.

    """
    _backend.update(email, initials=initials, messenger_id=messenger_id)
    logging.info(f"Updated user: {email=} {initials=} {messenger_id=}")


def set_status(email: str, status: str) -> None:
    """Set the status of a user, used for soft-deleting."""
    _backend.set_status(email, status)
    logging.info(f"Set user status: {email=} {status=}")
