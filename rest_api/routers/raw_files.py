"""Raw file endpoints."""

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from rest_api.dependencies import Pagination, RawFileQuery
from rest_api.schemas import Page, RawFileResponse
from rest_api.service import get_raw_files_with_metrics

router = APIRouter(prefix="/raw_files")


@router.get("/")
def list_raw_files(
    where: RawFileQuery,
    page: Annotated[Pagination, Depends()],
    *,
    include_metrics: Annotated[
        bool, Query(description="Include flattened metrics for each raw file")
    ] = True,
    include_file_info: Annotated[
        bool,
        Query(
            description="Include the file_info mapping and backup_path for each raw file"
        ),
    ] = False,
) -> Page[RawFileResponse]:
    """List raw files with optional filtering, including flattened metrics."""
    data, total = get_raw_files_with_metrics(
        where,
        page,
        include_metrics=include_metrics,
        include_file_info=include_file_info,
    )

    return Page(
        total=total,
        offset=page.offset,
        limit=page.limit,
        data=[RawFileResponse.of(item) for item in data],
    )
