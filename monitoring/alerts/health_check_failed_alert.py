"""Health check alert checker."""

from shared.db.models import KrakenStatus, KrakenStatusValues

from .base_alert import BaseAlert, Issue, get_instrument_id


class HealthCheckFailedAlert(BaseAlert):
    """Check for failed health checks."""

    def _get_issues(
        self, status_objects: list[KrakenStatus]
    ) -> list[Issue[str | None]]:
        """Check for health check failures."""
        health_check_failed_instruments = []

        for kraken_status in status_objects:
            if kraken_status.status != KrakenStatusValues.OK:
                id_ = kraken_status.id
                status_details = kraken_status.status_details
                health_check_failed_instruments.append(
                    Issue(
                        id_,
                        status_details,
                        instrument_id=get_instrument_id(kraken_status),
                    )
                )

        return health_check_failed_instruments

    def format_message(self, issues: list[Issue[str | None]]) -> str:
        """Format health check failure message."""
        instruments_str = "\n".join(
            [f"- `{issue.identifier}`: {issue.details}" for issue in issues]
        )
        return f"Health check failed:\n{instruments_str}"

    def get_cooldown_time_minutes(self, identifier: str) -> int | None:
        """Return custom cooldown for specific instruments."""
        if identifier == "file_remover":
            return 720  # 12 hours
        return super().get_cooldown_time_minutes(identifier)
