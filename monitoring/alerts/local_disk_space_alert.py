"""Local disk space alert checker."""

import logging
import shutil

from shared.db.models import KrakenStatus

from . import config
from .base_alert import BaseAlert
from .config import Cases


class LocalDiskSpaceAlert(BaseAlert):
    """Check the free space of the monitoring host's local disk.

    Unlike DiskSpaceAlert, this does not rely on KrakenStatus objects from the
    database but probes the local filesystem directly.
    """

    @property
    def name(self) -> str:
        """Return the case name for this alert type."""
        return Cases.LOCAL_DISK_SPACE

    def _get_issues(self, status_objects: list[KrakenStatus]) -> list[tuple[str, int]]:
        """Check for low local disk space."""
        del status_objects  # unused

        try:
            free_bytes = shutil.disk_usage(config.LOCAL_DISK_PATH).free
        except OSError:
            logging.exception(
                f"Failed to read local disk usage at {config.LOCAL_DISK_PATH}"
            )
            return []

        free_space_gb = int(free_bytes / config.BYTES_PER_GB)
        if free_space_gb < config.LOCAL_FREE_SPACE_THRESHOLD_GB:
            return [("local", free_space_gb)]

        return []

    def format_message(self, issues: list[tuple[str, int]]) -> str:
        """Format local disk space message."""
        free_space_gb = issues[0][1] if issues else "unknown"
        return (
            f"Low local disk space on the monitoring host "
            f"({config.LOCAL_DISK_PATH}): {free_space_gb} GB free"
        )
