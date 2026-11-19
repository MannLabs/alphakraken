"""Settings endpoints."""

from typing import Annotated

from fastapi import APIRouter, Depends, Query

from rest_api.dependencies import Pagination
from rest_api.schemas import Page, SettingsResponse
from rest_api.service import get_settings, get_settings_by_unique_name

router = APIRouter(prefix="/settings")


@router.get("/")
def list_settings(
    page: Annotated[Pagination, Depends()],
    name: Annotated[
        str | None, Query(description="Filter by settings name (exact match)")
    ] = None,
) -> Page[SettingsResponse]:
    """List non-deleted settings."""
    data, total = get_settings(name=name, limit=page.limit, offset=page.offset)
    return Page(
        total=total,
        offset=page.offset,
        limit=page.limit,
        data=[SettingsResponse.model_validate(s) for s in data],
    )


@router.get("/{settings_id}")
def read_settings(settings_id: str) -> SettingsResponse:
    """Get settings by ID, i.e. `<name>_v<version>`."""
    return SettingsResponse.model_validate(get_settings_by_unique_name(settings_id))
