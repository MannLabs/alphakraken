"""Base alert checker class."""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Generic, TypeVar

from alerts.config import (
    DEFAULT_ALERT_COOLDOWN_TIME_MINUTES,
    MAX_DETAILS_CHARS,
    Destinations,
)

from shared.db.models import KrakenStatus, KrakenStatusEntities

# covariant: Issue is immutable, so e.g. Issue[str] is an Issue[str | None]
D_co = TypeVar("D_co", covariant=True)


@dataclass(frozen=True)
class Issue(Generic[D_co]):
    """An issue found by an alert."""

    # e.g. an instrument or raw file id; key for cooldown and suppression context
    identifier: str
    # alert-specific payload, rendered by the alert's `format_message`
    details: D_co
    # raw file the issue relates to, if any
    relevant_file: str | None = None
    # instrument the issue relates to, if any
    instrument_id: str | None = None
    # stable per issue within an alert, must be an explicit string; empty: the identifier
    key: str = ""

    def __post_init__(self) -> None:
        """Default the key to the identifier."""
        if not self.key:
            object.__setattr__(self, "key", self.identifier)


def get_instrument_id(kraken_status: KrakenStatus) -> str | None:
    """Return the id of a status object if it belongs to an instrument, else None."""
    if kraken_status.entity_type == KrakenStatusEntities.INSTRUMENT:
        return kraken_status.id
    return None


class BaseAlert(ABC):
    """Base class for all alert checkers."""

    # message title; None if the body carries its own headers
    title: str | None
    # minimum time between two notifications of the same issue; None: notify only once
    cooldown_minutes: int | None = DEFAULT_ALERT_COOLDOWN_TIME_MINUTES
    # where the issues are sent to, cf. `Destinations`
    destinations: frozenset[str] = frozenset({Destinations.OPS})

    def get_issues(
        self, status_objects: list[KrakenStatus], now: datetime
    ) -> list[Issue]:
        """Check for issues and return them."""
        logging.info(f"Checking for issues: {self.name}...")
        issues = self._get_issues(status_objects, now)
        logging.info(f"Found {len(issues)} issues: {self.name}.")
        return issues

    @abstractmethod
    def _get_issues(
        self, status_objects: list[KrakenStatus], now: datetime
    ) -> list[Issue]:
        """Check for issues and return them."""

    def format_message(self, issues: list[Issue]) -> str:
        """Format the message body: one line per issue, with the details shortened.

        Override this method in subclasses that need a different format.
        """
        return "\n".join(
            f"- `{issue.identifier}`: {shorten(str(issue.details))}" for issue in issues
        )

    @property
    def name(self) -> str:
        """Return the name of this alert type: its class name."""
        return type(self).__name__

    def get_cooldown_minutes(self, issue: Issue) -> int | None:
        """Return the cooldown for an issue of this alert, None to notify it only once.

        Override this method in subclasses with a cooldown depending on the issue.
        """
        del issue  # unused
        return self.cooldown_minutes


def shorten(text: str, max_chars: int = MAX_DETAILS_CHARS) -> str:
    """Return the text cut to `max_chars` with an ellipsis, so that messages stay small."""
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1] + "…"
