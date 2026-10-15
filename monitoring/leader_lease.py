"""Leader election via a lease document in MongoDB, so that only one of several monitoring instances sends alerts."""

import logging
from datetime import datetime, timedelta

import pytz
from mongoengine.connection import get_db
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

LEASE_COLLECTION = "monitoring_leader_lease"
LEASE_ID = "monitoring"


class LeaderLease:
    """Lease that is held by at most one instance at a time and expires unless renewed."""

    def __init__(self, holder: str, lease_duration: timedelta) -> None:
        """Initialize with a holder id unique per instance."""
        self._holder = holder
        self._lease_duration = lease_duration
        self._is_leader = False

    def try_acquire(self) -> bool:
        """Acquire or renew the lease, return whether this instance is the leader.

        Note: expiry is compared against this instance's clock, so clocks of all instances must be roughly in sync.
        """
        now = datetime.now(pytz.UTC)
        try:
            # matches only if the lease is free or already ours; otherwise the upsert
            # tries to insert a second document with the same _id, which fails
            lease = get_db()[LEASE_COLLECTION].find_one_and_update(
                {
                    "_id": LEASE_ID,
                    "$or": [{"holder": self._holder}, {"expires_at": {"$lt": now}}],
                },
                {
                    "$set": {
                        "holder": self._holder,
                        "expires_at": now + self._lease_duration,
                    }
                },
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
            is_leader = lease["holder"] == self._holder
        except DuplicateKeyError:
            is_leader = False

        if is_leader != self._is_leader:
            logging.info(
                f"{'Acquired' if is_leader else 'Lost'} monitoring leadership ({self._holder})"
            )
        self._is_leader = is_leader
        return is_leader
