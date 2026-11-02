"""Stale status alert checker."""

from datetime import datetime, timedelta

from utils import ensure_utc

from shared.db.models import KrakenStatus, KrakenStatusEntities

from .base_alert import BaseAlert, Issue, get_instrument_id
from .config import (
    FILE_REMOVER_STALE_THRESHOLD_HOURS,
    STALE_STATUS_THRESHOLD_MINUTES,
)


class StaleStatusAlert(BaseAlert):
    """Check for stale status updates."""

    title = "Health check status is stale"

    def _get_issues(
        self, status_objects: list[KrakenStatus], now: datetime
    ) -> list[Issue[str]]:
        """Check for stale statuses."""
        stale_instruments = []
        for kraken_status in status_objects:
            last_updated_at = ensure_utc(kraken_status.updated_at_)
            id_ = kraken_status.id

            # Use different thresholds based on entry type
            if (
                kraken_status.entity_type == KrakenStatusEntities.JOB
                and id_ == "file_remover"
            ):
                time_delta = timedelta(hours=FILE_REMOVER_STALE_THRESHOLD_HOURS)
            else:
                time_delta = timedelta(minutes=STALE_STATUS_THRESHOLD_MINUTES)

            threshold = now - time_delta
            if last_updated_at < threshold:
                hours_ago = (now - last_updated_at).total_seconds() / 3600
                stale_instruments.append(
                    Issue(
                        id_,
                        f"{last_updated_at:%Y-%m-%d %H:%M:%S} UTC ({hours_ago:.1f} hours ago)",
                        instrument_id=get_instrument_id(kraken_status),
                    )
                )

        return stale_instruments
