"""Base alert checker class."""

import logging
from abc import ABC, abstractmethod
from collections.abc import Hashable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Generic, TypeVar

import yaml
from alerts.config import DEFAULT_ALERT_COOLDOWN_TIME_MINUTES

from shared.db.models import KrakenStatus, KrakenStatusEntities


def _load_suppressions() -> list[dict[str, Any]]:
    """Load suppressions from YAML file."""
    suppressions_file = Path(__file__).parent / ".." / "suppressions.yaml"

    try:
        with Path(suppressions_file).open() as f:
            config = yaml.safe_load(f)
            suppressions = config.get("suppressions", []) if config else []
            logging.info(f"Loaded {len(suppressions)} suppression rules")
    except FileNotFoundError:
        logging.info("No suppressions.yaml file found, no alerts will be suppressed")
        suppressions = []
    except Exception as e:  # noqa: BLE001
        logging.warning(f"Failed to load suppressions.yaml: {e}")
        suppressions = []
    return suppressions


WILDCARD = "*"

# Load suppressions at module import
_suppressions: list[dict[str, Any]] = _load_suppressions()

# covariant: Issue is immutable, so e.g. Issue[str] is an Issue[str | None]
D_co = TypeVar("D_co", covariant=True)


@dataclass(frozen=True)
class Issue(Generic[D_co]):
    """An issue found by an alert."""

    # e.g. an instrument or raw file id; key for cooldown and suppression context
    identifier: str
    # alert-specific payload, rendered by the alert's `format_message`
    details: D_co
    # raw file the issue relates to; its initials determine the user to notify
    relevant_file: str | None = None
    # instrument the issue relates to; matched exactly by suppressions
    instrument_id: str | None = None
    # distinguishes kinds of issues of one alert; part of the cooldown key
    kind: str | None = None
    # if set, the issue is reported only once per key, regardless of cooldown
    unique_key: Hashable | None = None


def get_instrument_id(kraken_status: KrakenStatus) -> str | None:
    """Return the id of a status object if it belongs to an instrument, else None."""
    if kraken_status.entity_type == KrakenStatusEntities.INSTRUMENT:
        return kraken_status.id
    return None


class BaseAlert(ABC):
    """Base class for all alert checkers."""

    def get_issues(self, status_objects: list[KrakenStatus]) -> list[Issue]:
        """Check for issues and return them.

        Returns empty list if no issues found.
        """
        logging.info(f"Checking for issues: {self.name}...")
        issues = self._get_issues(status_objects)
        issues_before_suppression = len(issues)
        issues = self._apply_suppressions(issues)

        if issues_before_suppression != len(issues):
            logging.info(
                f"Suppressed {issues_before_suppression - len(issues)} issues for {self.name}"
            )

        logging.info(f"Found {len(issues)} issues after suppression: {self.name}.")
        return issues

    def _apply_suppressions(self, issues: list[Issue]) -> list[Issue]:
        """Apply suppression rules to filter out suppressed issues."""
        if not _suppressions:
            return issues

        alert_class = self.__class__.__name__
        filtered_issues = []

        for issue in issues:
            context = issue.identifier
            message = issue.details

            suppressed = False
            for suppression in _suppressions:
                supp_instrument_id = suppression.get("instrument_id", WILDCARD)
                if suppression.get("error_class") in (WILDCARD, alert_class) and (
                    supp_instrument_id in (WILDCARD, issue.instrument_id)
                ):
                    supp_context = suppression.get("context", WILDCARD)
                    context_matches = supp_context in (WILDCARD, context)

                    supp_message = suppression.get("message", WILDCARD)
                    message_matches = supp_message == WILDCARD or supp_message in str(
                        message
                    )

                    if context_matches and message_matches:
                        suppressed = True
                        logging.debug(
                            f"Suppressed issue: {alert_class} - {context} - {message}"
                        )
                        break

            if not suppressed:
                filtered_issues.append(issue)

        return filtered_issues

    @abstractmethod
    def _get_issues(self, status_objects: list[KrakenStatus]) -> list[Issue]:
        """Check for issues and return them."""

    @abstractmethod
    def format_message(self, issues: list[Issue]) -> str:
        """Format the alert message for the issues found."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name for this alert type."""

    def get_cooldown_time_minutes(self, issue: Issue) -> int:
        """Return cooldown in minutes for an issue of this alert.

        Override this method in subclasses to specify custom cooldowns, e.g. per identifier or kind.
        """
        del issue  # unused
        return DEFAULT_ALERT_COOLDOWN_TIME_MINUTES


class CustomAlert(BaseAlert):
    """Simple alert wrapper for special alerts."""

    def __init__(self, alert_name: str, cooldown_time_minutes: int | None = None):
        """Initialize with alert name."""
        self._name = alert_name
        self._cooldown_time_minutes = cooldown_time_minutes

    @property
    def name(self) -> str:
        """Return the alert name."""
        return self._name

    def _get_issues(self, status_objects: list[KrakenStatus]) -> list[Issue]:
        """Not used for custom alerts."""
        del status_objects  # unused
        return []

    def format_message(self, issues: list[Issue]) -> str:
        """Not used for custom alerts."""
        del issues  # unused
        return ""

    def get_cooldown_time_minutes(self, issue: Issue) -> int:
        """Return cooldown in minutes for this alert."""
        if self._cooldown_time_minutes is not None:
            return self._cooldown_time_minutes
        return super().get_cooldown_time_minutes(issue)
