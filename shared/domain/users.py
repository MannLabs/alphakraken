"""User domain type and constants."""

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated

from pydantic import BeforeValidator, Field, StringConstraints, TypeAdapter

from shared.domain.inputs import (
    InputModel,
    OptionalStr,
    RequiredStr,
    empty_to_none,
    required,
)

USER_EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
USER_INITIALS_MAX_LENGTH = 8
# letters only, 2 to 8: what `INITIALS_PATTERN` in the monitoring can find in raw file names
USER_INITIALS_REGEX = rf"^[a-zA-Z]{{2,{USER_INITIALS_MAX_LENGTH}}}$"


# emails are stored lower-cased, so every email that is looked up is lower-cased as well
LowerCasedEmail = Annotated[
    str, StringConstraints(strip_whitespace=True, to_lower=True)
]
_EMAIL_ADAPTER = TypeAdapter(LowerCasedEmail)
# None if empty or blank
OptionalLowerCasedEmail = Annotated[
    LowerCasedEmail | None, BeforeValidator(empty_to_none)
]


def normalize_email(email: str) -> str:
    """Return the email as it is stored: stripped and lower-cased."""
    return _EMAIL_ADAPTER.validate_python(email)


class UserStatus:
    """Status of user."""

    ACTIVE = "active"
    INACTIVE = "inactive"


@dataclass(frozen=True)
class User:
    """A user, owner of projects and settings; not a login identity. Datetimes are UTC-aware."""

    email: str
    initials: str
    # user id in the messenger (e.g. Slack member ID), to notify the user directly
    messenger_id: str | None
    status: str
    created_at_: datetime


class UserUpdate(InputModel):
    """The changeable fields of a user, cf. `User`."""

    initials: RequiredStr = Field(
        max_length=USER_INITIALS_MAX_LENGTH, pattern=USER_INITIALS_REGEX
    )
    # required, so that leaving it out cannot clear it
    messenger_id: OptionalStr = Field(max_length=512)


class NewUser(UserUpdate):
    """A user to add, cf. `User`. The email is lower-cased."""

    email: Annotated[LowerCasedEmail, BeforeValidator(required())] = Field(
        max_length=128, pattern=USER_EMAIL_REGEX
    )
    messenger_id: OptionalStr = Field(default=None, max_length=512)
