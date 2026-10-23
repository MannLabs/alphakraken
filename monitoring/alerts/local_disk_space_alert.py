"""Local disk space alert checker."""

import logging
import shutil

from shared.db.models import KrakenStatus

from . import config
from .base_alert import BaseAlert, Issue


class LocalDiskSpaceAlert(BaseAlert):
    """Check the free space of the monitoring host's local disk.

    Unlike DiskSpaceAlert, this does not rely on KrakenStatus objects from the
    database but probes the local filesystem directly.
    """

    title = "Low local disk space on the monitoring host"

    def _get_issues(self, status_objects: list[KrakenStatus]) -> list[Issue[int]]:
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
            return [Issue("local", free_space_gb)]

        return []

    def format_message(self, issues: list[Issue[int]]) -> str:
        """Format local disk space message body."""
        return f"{config.LOCAL_DISK_PATH}: {issues[0].details} GB free"
