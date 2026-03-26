"""REST API endpoint definitions."""

from datetime import datetime
from typing import Annotated, Any

from fastapi import APIRouter, Query

from rest_api.schemas import RawFilesListResponse
from rest_api.service import get_raw_files_with_metrics

router = APIRouter()

MAX_LIMIT = 1000
DEFAULT_LIMIT = 100


@router.get("/raw_files/", response_model=RawFilesListResponse)
def list_raw_files(  # noqa: PLR0913
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
    limit: Annotated[
        int, Query(ge=1, le=MAX_LIMIT, description="Number of results per page")
    ] = DEFAULT_LIMIT,
    offset: Annotated[int, Query(ge=0, description="Number of results to skip")] = 0,
) -> dict[str, Any]:
    """List raw files with optional filtering, including flattened metrics."""
    data, total = get_raw_files_with_metrics(
        instrument_id=instrument_id,
        name_contains=name_contains,
        project_id=project_id,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
        offset=offset,
    )

    return {
        "total": total,
        "offset": offset,
        "limit": limit,
        "data": data,
    }
