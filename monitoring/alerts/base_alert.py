"""Base alert checker class."""

import logging
from abc import ABC, abstractmethod

from shared.db.models import KrakenStatus


class BaseAlert(ABC):
    """Base class for all alert checkers."""

    def get_issues(self, status_objects: list[KrakenStatus]) -> list[tuple]:
        """Check for issues and return list of (identifier, details) tuples.

        e.g. [('instrument1', "low disk space: 123 GB"), ...]

        Returns empty l.ist if no issues found.
        """
        logging.info(f"Checking for issues: {self.name}...")
        issues = self._get_issues(status_objects)
        logging.info(f"Found {len(issues)} issues: {self.name}.")
        return issues

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
