"""Election of the active instance via a lease in the database, so that only one of several monitoring instances runs the DB-backed checks."""

import logging
from datetime import datetime, timedelta

from shared.db import alerting_leases
from shared.domain.errors import DatabaseError

ACTIVE_INSTANCE_LEASE = "monitoring"


class ActiveInstanceElection:
    """Elects at most one active instance at a time; the claim expires unless renewed."""

    def __init__(self, instance: str, timeout: timedelta) -> None:
        """Initialize with an id unique per instance."""
        self._instance = instance
        self._timeout = timeout
        self._is_active = False

    def try_become_active(self, now: datetime) -> bool:
        """Claim or renew the active role, return whether this instance is the active one.

        Fails open: if the claim cannot be read or written (e.g. missing permissions), this
        instance acts as active one, as duplicate alerts are preferable to no alerts.

        Note: expiry is compared against this instance's clock, so clocks of all instances must be roughly in sync.
        """
        try:
            is_active = alerting_leases.try_acquire(
                ACTIVE_INSTANCE_LEASE, self._instance, ttl=self._timeout, now=now
            )
        except DatabaseError:
            logging.exception(
                "Failed to claim active alerting instance, acting as active one"
            )
            is_active = True

        if is_active != self._is_active:
            logging.info(
                f"{'Became' if is_active else 'No longer'} active alerting instance ({self._instance})"
            )
        self._is_active = is_active
        return is_active
