"""Disk space alert checker."""

from datetime import datetime

from shared.db.models import KrakenStatus, KrakenStatusEntities

from . import config
from .base_alert import BaseAlert, Issue, get_instrument_id


class DiskSpaceAlert(BaseAlert):
    """Check for low disk space."""

    title = "Low disk space detected"

    def _get_issues(
        self, status_objects: list[KrakenStatus], now: datetime
    ) -> list[Issue[int]]:
        """Check for low disk space."""
        del now  # unused
        low_disk_space_instruments = []

        for kraken_status in [
            o
            for o in status_objects
            if o.entity_type
            in [KrakenStatusEntities.INSTRUMENT, KrakenStatusEntities.FILE_SYSTEM]
        ]:
            if (free_space_gb := kraken_status.free_space_gb) < self._get_threshold(
                kraken_status
            ):
                low_disk_space_instruments.append(  # noqa: PERF401
                    Issue(
                        kraken_status.id,
                        free_space_gb,
                        instrument_id=get_instrument_id(kraken_status),
                    )
                )

        return low_disk_space_instruments

    @staticmethod
    def _get_threshold(kraken_status: KrakenStatus) -> int:
        """Get the threshold for a file system or instrument."""
        if kraken_status.entity_type == KrakenStatusEntities.FILE_SYSTEM:
            return config.FILE_SYSTEM_FREE_SPACE_THRESHOLDS_GB.get(
                kraken_status.id, config.FREE_SPACE_THRESHOLD_GB
            )
        return config.FREE_SPACE_THRESHOLD_GB

    def format_message(self, issues: list[Issue[int]]) -> str:
        """Format disk space message body."""
        return "\n".join(
            f"- `{issue.identifier}`: {issue.details} GB" for issue in issues
        )
