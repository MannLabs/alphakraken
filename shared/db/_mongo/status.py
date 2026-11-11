"""Kraken status queries on MongoDB."""

from datetime import UTC, datetime

from shared.db._mongo import models
from shared.db._mongo.connection import db_backend_function
from shared.db._mongo.mappers import to_kraken_status
from shared.domain.status import KrakenStatus, KrakenStatusEntities


@db_backend_function
def find() -> list[KrakenStatus]:
    """Return the status of all entities, cf. `shared.db.status`."""
    return [to_kraken_status(doc) for doc in models.KrakenStatus.objects]


@db_backend_function
def instrument_ids() -> list[str]:
    """Return the ids of all instruments, cf. `shared.db.status`."""
    query = models.KrakenStatus.objects(
        entity_type=KrakenStatusEntities.INSTRUMENT
    ).only("id")
    return [doc.id for doc in query]


@db_backend_function
def update(
    id_: str,
    *,
    status: str,
    status_details: str,
    free_space_gb: int,
    entity_type: str,
) -> None:
    """Set the status of an entity, cf. `shared.db.status`."""
    models.KrakenStatus(
        id=id_,
        status=status,
        updated_at_=datetime.now(tz=UTC),
        free_space_gb=free_space_gb,
        status_details=status_details,
        entity_type=entity_type,
    ).save()
