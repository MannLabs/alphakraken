"""User queries."""

import logging

from shared.db._mongo import users as _backend
from shared.domain.errors import ValidationError
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
    email = email.strip().lower()
    # moves to shared/services, cf. VALIDATION_DESIGN.md
    existing = find(include_inactive=True)
    if any(user.email == email for user in existing):
        raise ValidationError([f"A user with email '{email}' already exists."])
    _check_initials_unique(initials, existing)

    user = _backend.add(email=email, initials=initials, messenger_id=messenger_id)
    logging.info(f"Added user: {email=} {initials=} {messenger_id=}")
    return user


def update(email: str, *, initials: str, messenger_id: str | None) -> None:
    """Update the initials and messenger id of a user, the email is immutable.

    Raises:
        ValidationError: if another user has the initials.

    """
    # moves to shared/services, cf. VALIDATION_DESIGN.md
    others = [user for user in find(include_inactive=True) if user.email != email]
    _check_initials_unique(initials, others)

    _backend.update(email, initials=initials, messenger_id=messenger_id)
    logging.info(f"Updated user: {email=} {initials=} {messenger_id=}")


def _check_initials_unique(initials: str, users: list[User]) -> None:
    """Raise if one of the users has the initials."""
    if any(user.initials == initials for user in users):
        raise ValidationError([f"A user with initials '{initials}' already exists."])


def set_status(email: str, status: str) -> None:
    """Set the status of a user, used for soft-deleting."""
    _backend.set_status(email, status)
    logging.info(f"Set user status: {email=} {status=}")
