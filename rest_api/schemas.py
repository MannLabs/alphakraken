"""Pydantic response models for the REST API."""

from datetime import datetime
from typing import Any, Generic, Self, TypeVar

from pydantic import BaseModel, ConfigDict

from rest_api.service import ProjectWithAssignments, RawFileWithMetrics
from shared.domain.projects import ProjectSettings

T = TypeVar("T")


class Page(BaseModel, Generic[T]):
    """One page of a list."""

    # number of entries matching the filters, on all pages
    total: int
    offset: int
    limit: int
    data: list[T]


class RawFileResponse(BaseModel):
    """A raw file with its metrics."""

    model_config = ConfigDict(from_attributes=True)

    id: str
    collision_flag: str | None = None
    original_name: str
    instrument_id: str | None = None
    project_id: str | None = None
    status: str | None = None
    status_details: str | None = None
    size: int | None = None
    backup_status: str | None = None
    backup_display_base_path: str | None = None
    s3_upload_path: str | None = None
    instrument_file_status: str | None = None
    created_at: datetime | None = None
    finalized_at: datetime | None = None
    updated_at_: datetime | None = None
    metrics: list[dict[str, Any]] | None = None
    file_info: dict[str, Any] | None = None
    backup_path: str | None = None

    @classmethod
    def of(cls, item: RawFileWithMetrics) -> Self:
        """Build the response of a raw file with the data joined to it."""
        return cls.model_validate(item.raw_file).model_copy(
            update={"metrics": item.metrics, "backup_path": item.backup_path}
        )


class SettingsAssignmentResponse(BaseModel):
    """A settings assigned to a project, with the scopes and filters it applies to."""

    id: str
    project_id: str
    settings_id: str  # `<name>_v<version>`
    name: str
    version: int
    scopes: list[str]
    excluded_scopes: list[str]
    raw_file_id_filter: list[str]
    raw_file_id_exclude_filter: list[str]

    @classmethod
    def of(cls, assignment: ProjectSettings) -> Self:
        """Build the response of an assignment."""
        return cls(
            id=assignment.id,
            project_id=assignment.project_id,
            settings_id=assignment.settings.id,
            name=assignment.settings.name,
            version=assignment.settings.version,
            scopes=assignment.scopes,
            excluded_scopes=assignment.excluded_scopes,
            raw_file_id_filter=assignment.raw_file_id_filter,
            raw_file_id_exclude_filter=assignment.raw_file_id_exclude_filter,
        )


class ProjectResponse(BaseModel):
    """A project with its settings assignments."""

    id: str
    name: str
    description: str | None = None
    status: str
    owner_email: str | None = None
    created_at_: datetime | None = None
    settings: list[SettingsAssignmentResponse]

    @classmethod
    def of(cls, item: ProjectWithAssignments) -> Self:
        """Build the response of a project with its assignments."""
        project = item.project
        return cls(
            id=project.id,
            name=project.name,
            description=project.description,
            status=project.status,
            owner_email=project.owner_email,
            created_at_=project.created_at_,
            settings=[SettingsAssignmentResponse.of(a) for a in item.assignments],
        )


class SettingsResponse(BaseModel):
    """A settings entry."""

    model_config = ConfigDict(from_attributes=True)

    id: str  # `<name>_v<version>`
    number: int
    name: str
    version: int
    description: str | None = None
    fasta_file_name: str | None = None
    speclib_file_name: str | None = None
    config_file_name: str | None = None
    config_params: str | None = None
    # optional although the domain requires them: legacy entries have null here
    software_type: str | None = None
    software: str | None = None
    runner_name: str | None = None
    metrics_type: str | None = None
    slurm_cpus_per_task: int | None = None
    slurm_mem: str | None = None
    slurm_time: str | None = None
    num_threads: int | None = None
    status: str
    owner_email: str | None = None
    created_at_: datetime | None = None
