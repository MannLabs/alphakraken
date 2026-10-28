"""Pydantic response models for the REST API."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict


class RawFileResponse(BaseModel):
    """A raw file with its metrics."""

    model_config = ConfigDict(extra="allow")

    id: str
    original_name: str
    instrument_id: str | None = None
    project_id: str | None = None
    status: str | None = None
    # status_details: str | None = None
    size: int | None = None
    # backup_status: str | None = None
    # s3_upload_path: str | None = None
    # instrument_file_status: str | None = None
    created_at: datetime | None = None
    updated_at_: datetime | None = None
    metrics: list[dict[str, Any]] | None = None
    file_info: dict[str, Any] | None = None
    backup_path: str | None = None


class RawFilesListResponse(BaseModel):
    """Paginated list of raw files."""

    total: int
    offset: int
    limit: int
    data: list[RawFileResponse]


class SettingsAssignmentResponse(BaseModel):
    """A settings assigned to a project, with the scopes and filters it applies to."""

    settings_id: str  # `<name>_v<version>`
    name: str
    version: int
    scopes: list[str]
    excluded_scopes: list[str]
    raw_file_id_filter: list[str]
    raw_file_id_exclude_filter: list[str]


class ProjectResponse(BaseModel):
    """A project with its settings assignments."""

    id: str
    name: str
    description: str | None = None
    status: str
    created_at_: datetime | None = None
    settings: list[SettingsAssignmentResponse]


class ProjectsListResponse(BaseModel):
    """Paginated list of projects."""

    total: int
    offset: int
    limit: int
    data: list[ProjectResponse]


class SettingsResponse(BaseModel):
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
    # optional: fields added after the first release may be absent on legacy entries
    software_type: str | None = None
    software: str | None = None
    runner_name: str | None = None
    metrics_type: str | None = None
    slurm_cpus_per_task: int | None = None
    slurm_mem: str | None = None
    slurm_time: str | None = None
    num_threads: int | None = None
    status: str
    created_at_: datetime | None = None


class SettingsListResponse(BaseModel):
    """Paginated list of settings."""

    total: int
    offset: int
    limit: int
    data: list[SettingsResponse]
