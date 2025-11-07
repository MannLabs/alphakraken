"""Base alert checker class."""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import yaml
from alerts.config import OPS_ALERTS_WEBHOOK_URL

from monitoring.alerts.config import DEFAULT_ALERT_COOLDOWN_TIME_MINUTES
from shared.db.models import KrakenStatus


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


# Load suppressions at module import
_suppressions: list[dict[str, Any]] = _load_suppressions()


class BaseAlert(ABC):
    """Base class for all alert checkers."""

    def get_issues(self, status_objects: list[KrakenStatus]) -> list[tuple]:
        """Check for issues and return list of (identifier, details) tuples.

        e.g. [('instrument1', "low disk space: 123 GB"), ...]

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

    def _apply_suppressions(self, issues: list[tuple]) -> list[tuple]:
        """Apply suppression rules to filter out suppressed issues."""
        if not _suppressions:
            return issues

        alert_class = self.__class__.__name__
        filtered_issues = []

        for issue in issues:
            context = issue[0]
            message = issue[1]

            suppressed = False
            for suppression in _suppressions:
                if suppression.get("error_class") == alert_class:
                    supp_context = suppression.get("context", "*")
                    context_matches = supp_context == "*" or supp_context in context

                    supp_message = suppression.get("message", "*")
                    message_matches = supp_message == "*" or supp_message in message

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
    def _get_issues(self, status_objects: list[KrakenStatus]) -> list[tuple]:
        """Check for issues and return list of (identifier, details) tuples."""

    @abstractmethod
    def format_message(self, issues: list[tuple]) -> str:
        """Format the alert message for the issues found."""

    @property
    @abstractmethod
    def name(self) -> str:
        """Return the name for this alert type."""

    def get_webhook_url(self) -> str:
        """Return webhook URL for this alert.

        Override this method in subclasses to route alerts to different channels.
        Default implementation returns the global OPS_ALERTS_WEBHOOK_URL.
        """
        return OPS_ALERTS_WEBHOOK_URL

    def get_cooldown_time_minutes(self, identifier: str) -> int:
        """Return cooldown in minutes for this alert and identifier.

        Override this method in subclasses to specify custom cooldowns for specific identifiers.

        Args:
            identifier: The identifier (e.g., instrument_id) for this alert instance

        Returns:
            Cooldown in minutes

        """
        del identifier  # unused
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

    def _get_issues(self, status_objects: list[KrakenStatus]) -> list[tuple]:
        """Not used for custom alerts."""
        del status_objects  # unused
        return []

    def format_message(self, issues: list[tuple]) -> str:
        """Not used for custom alerts."""
        del issues  # unused
        return ""

    def get_cooldown_time_minutes(self, identifier: str) -> int:
        """Return cooldown in minutes for this alert."""
        if self._cooldown_time_minutes is not None:
            return self._cooldown_time_minutes
        return super().get_cooldown_time_minutes(identifier)
