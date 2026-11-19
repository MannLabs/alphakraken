"""Endpoints of the assignments of settings to projects."""

from fastapi import APIRouter, Depends, status

from rest_api.auth import require_api_key
from rest_api.schemas import SettingsAssignmentResponse
from shared.db import project_settings as project_settings_store
from shared.domain.projects import NewAssignment

router = APIRouter(prefix="/assignments")


@router.post(
    "/",
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_api_key)],
)
def create_assignment(new: NewAssignment) -> SettingsAssignmentResponse:
    """Assign settings to a project, for the given scopes and file name filters.

    All four lists are required: `scopes` needs at least one entry (`["*"]` for all
    instruments), the others may be empty.
    """
    assignment = project_settings_store.assign(
        project_id=new.project_id,
        settings_id=new.settings_id,
        allow_fallback=False,
        scopes=new.scopes,
        excluded_scopes=new.excluded_scopes,
        raw_file_id_filter=new.raw_file_id_filter,
        raw_file_id_exclude_filter=new.raw_file_id_exclude_filter,
    )
    return SettingsAssignmentResponse.of(assignment)
