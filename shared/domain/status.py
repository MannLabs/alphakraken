"""Kraken status domain type and constants."""

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated

from pydantic import BeforeValidator, Field

from shared.domain.inputs import InputModel, RequiredStr, truncated
from shared.keys import STATUS_DETAILS_MAX_LENGTH


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


class KrakenStatusUpdate(InputModel):
    """The status of an entity to set, cf. `KrakenStatus`; written by the pipeline only, so only the field limits are checked."""

    id: RequiredStr = Field(max_length=64)
    status: RequiredStr = Field(max_length=64)
    # truncated instead of rejected: an error message must not keep its status from being written
    status_details: Annotated[
        str,
        BeforeValidator(lambda value: truncated(value, STATUS_DETAILS_MAX_LENGTH)),
    ] = Field(max_length=STATUS_DETAILS_MAX_LENGTH)
    free_space_gb: int = Field(ge=-1)
    # cf. `KrakenStatusEntities`
    entity_type: RequiredStr = Field(max_length=16)
