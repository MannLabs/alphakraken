"""Base alert checker class."""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import yaml

from shared.db.models import KrakenStatus


def _load_suppressions() -> list[dict[str, Any]]:
    """Load suppressions from YAML file."""
    suppressions_file = Path(__file__).parent / "suppressions.yaml"

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
