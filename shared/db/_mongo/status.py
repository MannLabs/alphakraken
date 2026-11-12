"""Kraken status queries on MongoDB."""

from datetime import UTC, datetime

from shared.db import models
from shared.db._mongo.connection import ensure_connected
from shared.db._mongo.errors import translate_errors
from shared.db._mongo.mappers import to_kraken_status
from shared.domain.status import KrakenStatus, KrakenStatusEntities


@translate_errors
def find() -> list[KrakenStatus]:
    """Return the status of all entities, cf. `shared.db.status`."""
    ensure_connected()
    return [to_kraken_status(doc) for doc in models.KrakenStatus.objects]


@translate_errors
def instrument_ids() -> list[str]:
    """Return the ids of all instruments, cf. `shared.db.status`."""
    ensure_connected()
    query = models.KrakenStatus.objects(
        entity_type=KrakenStatusEntities.INSTRUMENT
    ).only("id")
    return [doc.id for doc in query]


@translate_errors
def update(
    id_: str,
    *,
    status: str,
    status_details: str,
    free_space_gb: int,
    entity_type: str,
) -> None:
    """Set the status of an entity, cf. `shared.db.status`."""
    ensure_connected()
    models.KrakenStatus(
        id=id_,
        status=status,
        updated_at_=datetime.now(tz=UTC),
        free_space_gb=free_space_gb,
        status_details=status_details,
        entity_type=entity_type,
    ).save()
