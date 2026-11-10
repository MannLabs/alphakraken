"""Kraken status queries."""

import logging

from shared.db._mongo import status as _backend
from shared.domain.status import KrakenStatus, KrakenStatusEntities


def find() -> list[KrakenStatus]:
    """Return the status of all entities."""
    return _backend.find()


def instrument_ids() -> list[str]:
    """Return the ids of all instruments known to the system."""
    return _backend.instrument_ids()


def update(
    id_: str,
    *,
    status: str,
    status_details: str,
    free_space_gb: int,
    entity_type: str = KrakenStatusEntities.INSTRUMENT,
) -> None:
    """Set the status of an entity, creating it if it does not exist."""
    logging.info(
        f"Updating DB: {id_=} to {status=} with {status_details=} {free_space_gb=} {entity_type=}"
    )
    _backend.update(
        id_,
        status=status,
        status_details=status_details,
        free_space_gb=free_space_gb,
        entity_type=entity_type,
    )
