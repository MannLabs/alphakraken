"""Health check alert checker."""

from datetime import datetime

from shared.db.models import KrakenStatus, KrakenStatusValues
from shared.keys import FILE_REMOVER_STATUS_ID

from .base_alert import BaseAlert, Issue, get_instrument_id
from .config import FILE_REMOVER_HEALTH_COOLDOWN_MINUTES


class HealthCheckFailedAlert(BaseAlert):
    """Check for failed health checks."""

    title = "Health check failed"

    def _get_issues(
        self, status_objects: list[KrakenStatus], now: datetime
    ) -> list[Issue[str | None]]:
        """Check for health check failures."""
        del now  # unused
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

    def get_cooldown_minutes(self, issue: Issue) -> int | None:
        """Return a longer cooldown for the file remover."""
        if issue.identifier == FILE_REMOVER_STATUS_ID:
            return FILE_REMOVER_HEALTH_COOLDOWN_MINUTES
        return self.cooldown_minutes
