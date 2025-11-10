"""Webapp health alert checker."""

import logging

import requests
import requests.exceptions

from shared.db.models import KrakenStatus
from shared.yamlsettings import YamlKeys, get_notification_setting

from .base_alert import BaseAlert
from .config import Cases


class WebAppHealthAlert(BaseAlert):
    """Check if the webapp is accessible and responding with HTTP 200."""

    def __init__(self, timeout: int = 10):
        """Initialize with optional timeout for HTTP requests."""
        self.timeout = timeout
        try:
            self.webapp_url = get_notification_setting(YamlKeys.WEBAPP_URL)
        except KeyError:
            logging.warning("WEBAPP_URL not found in config, health check disabled")
            self.webapp_url = ""

    @property
    def name(self) -> str:
        """Return the case name for this alert type."""
        return Cases.WEBAPP_HEALTH

    def _get_issues(self, status_objects: list[KrakenStatus]) -> list[tuple[str, str]]:
        """Check if webapp is accessible via HTTP request."""
        del status_objects  # unused

        if not self.webapp_url:
            logging.warning("WEBAPP_URL not configured, skipping webapp health check")
            return []

        issues = []

        try:
            response = requests.get(self.webapp_url, timeout=self.timeout)
            if response.status_code != 200:  # noqa: PLR2004
                issues.append(
                    ("webapp", f"HTTP {response.status_code}: {response.reason}")
                )
        except requests.exceptions.Timeout:
            issues.append(("webapp", f"Request timeout after {self.timeout} seconds"))
        except requests.exceptions.ConnectionError:
            issues.append(("webapp", "Connection failed - webapp may be down"))
        except requests.exceptions.RequestException as e:
            issues.append(("webapp", f"Request failed: {e!s}"))
        except Exception as e:
            logging.exception("Unexpected error during webapp health check")
            issues.append(("webapp", f"Unexpected error: {e!s}"))

        return issues

    def format_message(self, issues: list[tuple[str, str]]) -> str:
        """Format webapp health failure message."""
        error_details = issues[0][1] if issues else "Unknown error"
        return f"Webapp health check failed: {error_details} (URL: {self.webapp_url})"
