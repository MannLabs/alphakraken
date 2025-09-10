"""Disk space alert checker."""

import config
from config import Cases

from shared.db.models import KrakenStatus, KrakenStatusEntities

from .base_alert import BaseAlert


class DiskSpaceAlert(BaseAlert):
    """Check for low disk space."""

    @property
    def case_name(self) -> str:
        """Return the case name for this alert type."""
        return Cases.LOW_DISK_SPACE

    def check(self, status_objects: list[KrakenStatus]) -> list[tuple[str, int]]:
        """Check for low disk space."""
        low_disk_space_instruments = []

        for kraken_status in status_objects:
            # Skip job-type entries
            if kraken_status.entity_type == KrakenStatusEntities.JOB:
                continue

            # Determine threshold based on entry type
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

            if (free_space_gb := kraken_status.free_space_gb) < threshold:
                low_disk_space_instruments.append((id_, free_space_gb))

        return low_disk_space_instruments

    def format_message(self, issues: list[tuple[str, int]]) -> str:
        """Format disk space message."""
        instruments_str = "\n".join(
            [
                f"- `{instrument_id}`: {disk_space} GB"
                for instrument_id, disk_space in issues
            ]
        )
        return f"Low disk space detected:\n{instruments_str}"
