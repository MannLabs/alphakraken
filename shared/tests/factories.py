"""Factories for domain objects, with plausible defaults that tests override per field."""

from datetime import UTC, datetime
from typing import Any

from shared.domain.raw_files import InstrumentFileStatus, RawFile, RawFileStatus
from shared.keys import FALLBACK_PROJECT_ID

SOME_TIME = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)


def raw_file(**overrides: Any) -> RawFile:
    """Return a raw file in DONE status."""
    values: dict[str, Any] = {
        "id": "file.raw",
        "collision_flag": None,
        "original_name": "file.raw",
        "instrument_id": "inst1",
        "project_id": FALLBACK_PROJECT_ID,
        "status": RawFileStatus.DONE,
        "status_details": None,
        "size": 1024,
        "backup_status": None,
        "backup_display_base_path": None,
        "s3_upload_path": None,
        "instrument_file_status": InstrumentFileStatus.NA,
        "file_info": None,
        "created_at": SOME_TIME,
        "finalized_at": None,
        "created_at_": SOME_TIME,
        "updated_at_": SOME_TIME,
    }
    return RawFile(**{**values, **overrides})
