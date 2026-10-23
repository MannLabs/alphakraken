"""Election of the active instance via a document in MongoDB, so that only one of several monitoring instances runs the DB-backed checks."""

import logging
from datetime import datetime, timedelta

from mongoengine.connection import get_db
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

ACTIVE_INSTANCE_COLLECTION = "alerting_active_instance"
ACTIVE_INSTANCE_DOCUMENT_ID = "monitoring"


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
            # matches only if the claim is expired or already ours; otherwise the upsert
            # tries to insert a second document with the same _id, which fails
            claim = get_db()[ACTIVE_INSTANCE_COLLECTION].find_one_and_update(
                {
                    "_id": ACTIVE_INSTANCE_DOCUMENT_ID,
                    "$or": [
                        {"instance": self._instance},
                        {"expires_at": {"$lt": now}},
                    ],
                },
                {
                    "$set": {
                        "instance": self._instance,
                        "expires_at": now + self._timeout,
                    }
                },
                upsert=True,
                return_document=ReturnDocument.AFTER,
            )
            is_active = claim["instance"] == self._instance
        except DuplicateKeyError:
            is_active = False
        except PyMongoError:
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
