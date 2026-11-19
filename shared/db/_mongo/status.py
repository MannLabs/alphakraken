"""Kraken status queries on MongoDB."""

from datetime import UTC, datetime

from shared.db._mongo import models
from shared.db._mongo.connection import db_backend_function
from shared.db._mongo.mappers import to_kraken_status
from shared.domain.status import KrakenStatus, KrakenStatusEntities, KrakenStatusUpdate


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
def update(new: KrakenStatusUpdate) -> None:
    """Set the status of an entity, cf. `shared.db.status`."""
    models.KrakenStatus(
        id=new.id,
        status=new.status,
        updated_at_=datetime.now(tz=UTC),
        free_space_gb=new.free_space_gb,
        status_details=new.status_details,
        entity_type=new.entity_type,
    ).save()
