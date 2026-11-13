"""User domain type and constants."""

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated

from pydantic import Field, StringConstraints, field_validator

from shared.domain.inputs import InputModel, empty_to_none, required

USER_EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
USER_INITIALS_MAX_LENGTH = 8
# letters only, 2 to 8: what `INITIALS_PATTERN` in the monitoring can find in raw file names
USER_INITIALS_REGEX = rf"^[a-zA-Z]{{2,{USER_INITIALS_MAX_LENGTH}}}$"


class UserStatus:
    """Status of user."""

    ACTIVE = "active"
    INACTIVE = "inactive"


# the statuses that may follow each status
USER_STATUS_TRANSITIONS: dict[str, set[str]] = {
    UserStatus.ACTIVE: {UserStatus.INACTIVE},
    UserStatus.INACTIVE: {UserStatus.ACTIVE},
}


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

    initials: str = Field(
        max_length=USER_INITIALS_MAX_LENGTH, pattern=USER_INITIALS_REGEX
    )
    # required, so that leaving it out cannot clear it
    messenger_id: str | None = Field(max_length=64)

    _required = field_validator("initials", mode="before")(required())
    _empty_to_none = field_validator("messenger_id", mode="before")(empty_to_none)


class NewUser(UserUpdate):
    """A user to add, cf. `User`. The email is lower-cased."""

    email: Annotated[
        str,
        StringConstraints(to_lower=True, max_length=128, pattern=USER_EMAIL_REGEX),
    ]
    messenger_id: str | None = Field(default=None, max_length=64)

    _email_required = field_validator("email", mode="before")(required())
