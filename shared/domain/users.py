"""User domain type and constants."""

from dataclasses import dataclass
from datetime import datetime


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
