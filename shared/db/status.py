"""Kraken status queries."""

from shared.db._mongo import status as _backend


def instrument_ids() -> list[str]:
    """Return the ids of all instruments known to the system."""
    return _backend.instrument_ids()
