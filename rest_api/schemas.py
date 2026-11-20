"""Pydantic response models for the REST API."""

from datetime import datetime
from typing import Any, Generic, Self, TypeVar

from pydantic import BaseModel

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

    @classmethod
    def of_all(cls, data: list[T]) -> Self:
        """Build the one page of a list that has no paging: it holds all entries."""
        return cls(total=len(data), offset=0, limit=len(data), data=data)


class DomainResponse(BaseModel):
    """A response built from a domain object."""

    @classmethod
    def of(cls, entity: object, **joined: Any) -> Self:
        """Build and validate the response from the fields of a domain object and the data joined to it.

        Fields of the domain object that the response does not declare are left out.
        """
        return cls.model_validate({**vars(entity), **joined})


class RawFileResponse(DomainResponse):
    """A raw file with its metrics."""

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
    def of_item(cls, item: RawFileWithMetrics) -> Self:
        """Build the response of a raw file with the data joined to it."""
        return cls.of(item.raw_file, metrics=item.metrics, backup_path=item.backup_path)


class SettingsAssignmentResponse(DomainResponse):
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
    def of(cls, entity: ProjectSettings, **joined: Any) -> Self:
        """Build the response of an assignment, with its settings as their id, name and version."""
        return super().of(
            entity,
            settings_id=entity.settings.id,
            name=entity.settings.name,
            version=entity.settings.version,
            **joined,
        )


class ProjectResponse(DomainResponse):
    """A project with its settings assignments."""

    id: str
    name: str
    description: str | None = None
    status: str
    owner_email: str | None = None
    created_at_: datetime | None = None
    settings: list[SettingsAssignmentResponse]

    @classmethod
    def of_item(cls, item: ProjectWithAssignments) -> Self:
        """Build the response of a project with its assignments."""
        return cls.of(
            item.project,
            settings=[SettingsAssignmentResponse.of(a) for a in item.assignments],
        )


class SettingsResponse(DomainResponse):
    """A settings entry."""

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


class StatusResponse(DomainResponse):
    """Health of an entity (instrument, file system, job) as last reported."""

    id: str
    status: str
    status_details: str | None = None
    free_space_gb: int
    entity_type: str
    updated_at_: datetime


class UserResponse(DomainResponse):
    """A user who can own projects and settings."""

    email: str
    initials: str


class ErrorResponse(BaseModel):
    """An error with one message."""

    detail: str


class InvalidInputResponse(BaseModel):
    """All violations of a request: `<field or location>: <message>`, or the bare message for a rule spanning several fields."""

    detail: list[str]


# `responses=` of the routes, so that OpenAPI documents the errors as they are returned
UNAUTHORIZED: dict[int | str, dict[str, Any]] = {
    401: {"model": ErrorResponse, "description": "The API key is missing or wrong"}
}
NOT_FOUND: dict[int | str, dict[str, Any]] = {
    404: {
        "model": ErrorResponse,
        "description": "The record in the path does not exist",
    }
}
CONFLICT: dict[int | str, dict[str, Any]] = {
    409: {"model": ErrorResponse, "description": "A record with the same key exists"}
}
INVALID_INPUT: dict[int | str, dict[str, Any]] = {
    422: {"model": InvalidInputResponse, "description": "The request is invalid"}
}
