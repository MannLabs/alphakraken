"""Kraken status queries on MongoDB."""

from shared.db import models
from shared.db._mongo.connection import ensure_connected
from shared.db._mongo.errors import translate_errors
from shared.domain.status import KrakenStatusEntities


@translate_errors
def instrument_ids() -> list[str]:
    """Return the ids of all instruments, cf. `shared.db.status`."""
    ensure_connected()
    query = models.KrakenStatus.objects(
        entity_type=KrakenStatusEntities.INSTRUMENT
    ).only("id")
    return [doc.id for doc in query]
