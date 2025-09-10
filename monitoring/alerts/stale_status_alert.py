"""Stale status alert checker."""

from datetime import datetime, timedelta

import config
import pytz
from config import Cases

from shared.db.models import KrakenStatus, KrakenStatusEntities

from .base_alert import BaseAlert


class StaleStatusAlert(BaseAlert):
    """Check for stale status updates."""

    @property
    def name(self) -> str:
        """Return the case name for this alert type."""
        return Cases.STALE

    def get_issues(
        self, status_objects: list[KrakenStatus]
    ) -> list[tuple[str, datetime]]:
        """Check for stale statuses."""
        now = datetime.now(pytz.UTC)

        stale_instruments = []
        for kraken_status in status_objects:
            last_updated_at = pytz.utc.localize(kraken_status.updated_at_)
            id_ = kraken_status.id

            # Use different thresholds based on entry type
            if (
                kraken_status.entity_type == KrakenStatusEntities.JOB
                and id_ == "file_remover"
            ):
                time_delta = timedelta(hours=config.FILE_REMOVER_STALE_THRESHOLD_HOURS)
            else:
                time_delta = timedelta(minutes=config.STALE_STATUS_THRESHOLD_MINUTES)

            threshold = now - time_delta
            if last_updated_at < threshold:
                stale_instruments.append((id_, last_updated_at))

        return stale_instruments

    def format_message(self, issues: list[tuple[str, datetime]]) -> str:
        """Format stale status message."""
        instruments_str = "\n".join(
            [
                f"- `{instrument_id}`: {updated_at.strftime('%Y-%m-%d %H:%M:%S')} UTC "
                f"({(datetime.now(pytz.UTC) - updated_at).total_seconds() / 60 / 60:.1f} hours ago)"
                for instrument_id, updated_at in issues
            ]
        )
        return f"Health check status is stale:\n{instruments_str}."
