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
    metrics: list[dict[str, Any]] = []


class RawFilesListResponse(BaseModel):
    """Paginated list of raw files."""

    total: int
    offset: int
    limit: int
    data: list[RawFileResponse]
