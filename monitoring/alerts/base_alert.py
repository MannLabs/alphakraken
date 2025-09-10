"""Base alert checker class."""

from abc import ABC, abstractmethod

from shared.db.models import KrakenStatus


class BaseAlert(ABC):
    """Base class for all alert checkers."""

    @abstractmethod
    def get_issues(self, status_objects: list[KrakenStatus]) -> list[tuple]:
        """Check for issues and return list of (identifier, details) tuples.

        Returns empty list if no issues found.
        """

    @abstractmethod
    def format_message(self, issues: list[tuple]) -> str:
        """Format the alert message for the issues found."""

    @property
    @abstractmethod
    def case_name(self) -> str:
        """Return the case name for this alert type."""
