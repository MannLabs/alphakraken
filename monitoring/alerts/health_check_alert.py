"""Health check alert checker."""

from config import Cases

from shared.db.models import KrakenStatus, KrakenStatusValues

from .base_alert import BaseAlert


class HealthCheckAlert(BaseAlert):
    """Check for failed health checks."""

    @property
    def name(self) -> str:
        """Return the case name for this alert type."""
        return Cases.HEALTH_CHECK_FAILED

    def get_issues(self, status_objects: list[KrakenStatus]) -> list[tuple[str, str]]:
        """Check for health check failures."""
        health_check_failed_instruments = []

        for kraken_status in status_objects:
            if kraken_status.status != KrakenStatusValues.OK:
                id_ = kraken_status.id
                status_details = kraken_status.status_details
                health_check_failed_instruments.append((id_, status_details))

        return health_check_failed_instruments

    def format_message(self, issues: list[tuple[str, str]]) -> str:
        """Format health check failure message."""
        instruments_str = "\n".join(
            [
                f"- `{instrument_id}`: {status_details}"
                for instrument_id, status_details in issues
            ]
        )
        return f"Health check failed:\n{instruments_str}"
