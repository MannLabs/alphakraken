"""Settings endpoints."""

from typing import Annotated

from fastapi import APIRouter, Depends, Query, status

from rest_api.auth import require_api_key
from rest_api.dependencies import Pagination
from rest_api.schemas import (
    CONFLICT,
    NOT_FOUND,
    UNAUTHORIZED,
    Page,
    SettingsResponse,
)
from rest_api.service import get_settings, get_settings_by_unique_name
from shared.db import settings as settings_store
from shared.domain.settings import NewSettings

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


@router.get("/{settings_id}", responses=NOT_FOUND)
def read_settings(settings_id: str) -> SettingsResponse:
    """Get settings by ID, i.e. `<name>_v<version>`."""
    return SettingsResponse.model_validate(get_settings_by_unique_name(settings_id))


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_api_key)],
    responses={**UNAUTHORIZED, **CONFLICT},
)
def create_settings(
    new: NewSettings,
    *,
    archive_previous: Annotated[
        bool,
        Query(
            description="Archive the latest active version of the name after creating the new one"
        ),
    ] = False,
) -> SettingsResponse:
    """Create settings, as the next version of the settings with the same name.

    Missing resource parameters (`slurm_*`, `num_threads`) get the defaults of the software type.
    `archive_previous` archives the latest *active* version of the name; a newer version that
    is already archived stays as it is.
    """
    created = settings_store.create(
        name=new.name,
        software_type=new.software_type,
        software=new.software,
        runner_name=new.runner_name,
        metrics_type=new.metrics_type,
        description=new.description,
        fasta_file_name=new.fasta_file_name,
        speclib_file_name=new.speclib_file_name,
        config_file_name=new.config_file_name,
        config_params=new.config_params,
        slurm_cpus_per_task=new.slurm_cpus_per_task,
        slurm_mem=new.slurm_mem,
        slurm_time=new.slurm_time,
        num_threads=new.num_threads,
        owner_email=new.owner_email,
        archive_previous=archive_previous,
    )
    return SettingsResponse.model_validate(created)


@router.post(
    "/{settings_id}/archive",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(require_api_key)],
    responses={**UNAUTHORIZED, **NOT_FOUND},
)
def archive_settings(settings_id: str) -> None:
    """Archive active settings."""
    settings_store.archive(settings_id)


@router.delete(
    "/{settings_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(require_api_key)],
    responses={**UNAUTHORIZED, **NOT_FOUND},
)
def delete_settings(settings_id: str) -> None:
    """Delete settings that are not assigned to an active project: they are kept in the database, but no longer returned."""
    settings_store.delete(settings_id)
