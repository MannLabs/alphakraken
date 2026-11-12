"""User domain type and constants."""

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

from shared.domain.inputs import empty_to_none

USER_EMAIL_REGEX = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"
# letters only, 2 to 8: what `INITIALS_PATTERN` in the monitoring can find in raw file names
USER_INITIALS_REGEX = r"^[a-zA-Z]{2,8}$"


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


class UserUpdate(BaseModel):
    """The changeable fields of a user, cf. `User`. Strings are stripped."""

    model_config = ConfigDict(frozen=True, str_strip_whitespace=True)

    initials: str = Field(pattern=USER_INITIALS_REGEX)
    messenger_id: str | None = Field(default=None, max_length=64)

    _empty_to_none = field_validator("messenger_id", mode="before")(empty_to_none)


class NewUser(UserUpdate):
    """A user to add, cf. `User`. The email is lower-cased."""

    email: Annotated[
        str,
        StringConstraints(to_lower=True, max_length=128, pattern=USER_EMAIL_REGEX),
    ]
