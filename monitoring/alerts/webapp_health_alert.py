"""Webapp health alert checker."""

import logging
from datetime import datetime
from http import HTTPStatus

import requests
import requests.exceptions

from shared.db.models import KrakenStatus
from shared.yamlsettings import YamlKeys, get_notification_setting

from .base_alert import BaseAlert, Issue


class WebAppHealthAlert(BaseAlert):
    """Check if the webapp is accessible and responding with HTTP 200."""

    title = "Webapp health check failed"

    def __init__(self, timeout: int = 10):
        """Initialize with optional timeout for HTTP requests."""
        self.timeout = timeout
        try:
            self.webapp_url = f"{get_notification_setting(YamlKeys.WEBAPP_URL).rstrip('/')}/healthz"  # built-in streamlit endpoint
        except KeyError:
            logging.warning("WEBAPP_URL not found in config, health check disabled")
            self.webapp_url = ""

    def _get_issues(
        self, status_objects: list[KrakenStatus], now: datetime
    ) -> list[Issue[str]]:
        """Check if webapp is accessible via HTTP request."""
        del status_objects, now  # unused

        if not self.webapp_url:
            logging.warning("WEBAPP_URL not configured, skipping webapp health check")
            return []

        try:
            response = requests.get(self.webapp_url, timeout=self.timeout)
        except requests.exceptions.RequestException as e:
            return [Issue("webapp", f"{type(e).__name__}: {e}")]

        if response.status_code != HTTPStatus.OK:
            return [Issue("webapp", f"HTTP {response.status_code}: {response.reason}")]
        return []

    def format_message(self, issues: list[Issue[str]]) -> str:
        """Format webapp health failure message body."""
        error_details = issues[0].details if issues else "Unknown error"
        return f"{error_details} (URL: {self.webapp_url})"
