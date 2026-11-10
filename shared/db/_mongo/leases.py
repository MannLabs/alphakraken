"""Leases on MongoDB: one document per lease."""

from datetime import datetime, timedelta

from mongoengine.connection import get_db
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from shared.db._mongo.connection import ensure_connected
from shared.db._mongo.errors import translate_errors

# named after its first use; the alerting role has write access to it, cf. misc/init-mongo.sh
LEASE_COLLECTION = "alerting_active_instance"
HOLDER_FIELD = "instance"
EXPIRES_AT_FIELD = "expires_at"


@translate_errors
def try_acquire(name: str, holder: str, *, ttl: timedelta, now: datetime) -> bool:
    """Claim or renew a lease, cf. `shared.db.leases`."""
    ensure_connected()
    try:
        # matches only if the lease is expired or already ours; otherwise the upsert
        # tries to insert a second document with the same _id, which fails
        lease = get_db()[LEASE_COLLECTION].find_one_and_update(
            {
                "_id": name,
                "$or": [
                    {HOLDER_FIELD: holder},
                    {EXPIRES_AT_FIELD: {"$lt": now}},
                ],
            },
            {"$set": {HOLDER_FIELD: holder, EXPIRES_AT_FIELD: now + ttl}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
    except DuplicateKeyError:
        return False
    return lease[HOLDER_FIELD] == holder
