"""User domain type and constants."""

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated

import pydantic
from pydantic import (
    AfterValidator,
    BeforeValidator,
    Field,
    StringConstraints,
    TypeAdapter,
)

from shared.domain.errors import InvalidInputError
from shared.domain.inputs import (
    InputModel,
    OptionalStr,
    RequiredStr,
    empty_to_none,
    matching,
    required,
)

# visible ASCII characters only (`!` to `~` without `@`), so that no invisible character or
# lookalike letter makes a second user with the same-looking email
USER_EMAIL_REGEX = r"^[!-?A-~]+@[!-?A-~]+\.[!-?A-~]+$"
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
    """Return the email as it is stored: stripped and lower-cased.

    Raises:
        InvalidInputError: if it is not a string.

    """
    try:
        return _EMAIL_ADAPTER.validate_python(email)
    except pydantic.ValidationError as e:
        raise InvalidInputError(["email: must be a string."]) from e


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

    initials: Annotated[
        RequiredStr,
        AfterValidator(matching(USER_INITIALS_REGEX, "must be 2 to 8 letters.")),
    ] = Field(
        max_length=USER_INITIALS_MAX_LENGTH,
        json_schema_extra={"pattern": USER_INITIALS_REGEX},
    )
    messenger_id: OptionalStr = Field(default=None, max_length=512)


class NewUser(UserUpdate):
    """A user to add, cf. `User`. The email is lower-cased."""

    email: Annotated[
        LowerCasedEmail,
        BeforeValidator(required()),
        AfterValidator(
            matching(
                USER_EMAIL_REGEX,
                "must be an email address, e.g. 'jane.doe@example.org'.",
            )
        ),
    ] = Field(max_length=128, json_schema_extra={"pattern": USER_EMAIL_REGEX})
