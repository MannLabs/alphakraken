"""Kraken status domain type and constants."""

from dataclasses import dataclass
from datetime import datetime


class KrakenStatusValues:
    """Values of kraken health status."""

    OK = "ok"
    ERROR = "error"


class KrakenStatusEntities:
    """Entities for AlphaKraken status updates."""

    INSTRUMENT = "instrument"
    FILE_SYSTEM = "file_system"
    JOB = "job"


@dataclass(frozen=True)
class KrakenStatus:
    """Health of an entity (instrument, file system, job) as last reported. Datetimes are UTC-aware."""

    id: str
    status: str
    status_details: str | None
    free_space_gb: int
    # cf. `KrakenStatusEntities`
    entity_type: str
    updated_at_: datetime
