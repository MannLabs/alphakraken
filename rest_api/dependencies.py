"""Query parameters shared by the routes of the REST API."""

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated

from fastapi import Depends, Query

from shared.db.raw_files import RawFileFilter

MAX_LIMIT = 1000
DEFAULT_LIMIT = 100


@dataclass(frozen=True)
class Pagination:
    """The page of a list to return."""

    limit: Annotated[
        int, Query(ge=1, le=MAX_LIMIT, description="Number of results per page")
    ] = DEFAULT_LIMIT
    offset: Annotated[int, Query(ge=0, description="Number of results to skip")] = 0


def _raw_file_filter(
    instrument_id: Annotated[
        str | None, Query(description="Filter by instrument ID (exact match)")
    ] = None,
    name_contains: Annotated[
        str | None,
        Query(description="Case-insensitive substring search on raw file ID"),
    ] = None,
    project_id: Annotated[
        str | None, Query(description="Filter by project ID (exact match)")
    ] = None,
    date_from: Annotated[
        datetime | None, Query(description="Filter: created_at >= date_from (ISO 8601)")
    ] = None,
    date_to: Annotated[
        datetime | None, Query(description="Filter: created_at <= date_to (ISO 8601)")
    ] = None,
) -> RawFileFilter:
    return RawFileFilter(
        instrument_id=instrument_id,
        name_contains=name_contains,
        project_id=project_id,
        created_after=date_from,
        created_before=date_to,
    )


RawFileQuery = Annotated[RawFileFilter, Depends(_raw_file_filter)]
