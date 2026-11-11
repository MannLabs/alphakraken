"""User domain type and constants."""

from dataclasses import dataclass
from datetime import datetime

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
