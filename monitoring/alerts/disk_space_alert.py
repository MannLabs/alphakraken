"""Disk space alert checker."""

from shared.db.models import KrakenStatus, KrakenStatusEntities

from . import config
from .base_alert import BaseAlert
from .config import Cases


class DiskSpaceAlert(BaseAlert):
    """Check for low disk space."""

    @property
    def name(self) -> str:
        """Return the case name for this alert type."""
        return Cases.LOW_DISK_SPACE

    def get_issues(self, status_objects: list[KrakenStatus]) -> list[tuple[str, int]]:
        """Check for low disk space."""
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
                low_disk_space_instruments.append((kraken_status.id, free_space_gb))  # noqa: PERF401

        return low_disk_space_instruments

    @staticmethod
    def _get_threshold(kraken_status: KrakenStatus) -> int:
        """Get the appropriate threshold based on entity type and ID."""
        id_ = kraken_status.id

        if kraken_status.entity_type == KrakenStatusEntities.FILE_SYSTEM:
            if id_ == "backup":
                threshold = config.BACKUP_FREE_SPACE_THRESHOLD_GB
            elif id_ == "output":
                threshold = config.OUTPUT_FREE_SPACE_THRESHOLD_GB
            else:
                threshold = config.FREE_SPACE_THRESHOLD_GB
        else:
            threshold = config.FREE_SPACE_THRESHOLD_GB

        return threshold

    def format_message(self, issues: list[tuple[str, int]]) -> str:
        """Format disk space message."""
        instruments_str = "\n".join(
            [
                f"- `{instrument_id}`: {disk_space} GB"
                for instrument_id, disk_space in issues
            ]
        )
        return f"Low disk space detected:\n{instruments_str}"
