"""Leases: exclusive, expiring claims, e.g. to elect one of several instances."""

from datetime import datetime, timedelta

from shared.db._mongo import leases as _backend


def try_acquire(name: str, holder: str, *, ttl: timedelta, now: datetime) -> bool:
    """Claim or renew the lease `name` for `holder` until `now + ttl`.

    :return: whether `holder` holds the lease now; False if another holder's claim has not expired

    Raises:
        DatabaseError: if the lease cannot be read or written.

    """
    return _backend.try_acquire(name, holder, ttl=ttl, now=now)
