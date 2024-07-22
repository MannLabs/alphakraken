"""Tests for the settings module."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytz
from common.settings import get_output_folder_rel_path
from db.models import RawFile


def test_get_output_folder_rel_path_no_fallback() -> None:
    """Test that correct output folder is returned if project_id is given."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id="some_project_id",
    )
    mock_raw_file.name = "some_file.raw"

    # when
    result = get_output_folder_rel_path(mock_raw_file, "some_project_id")

    assert result == Path("output/some_project_id/out_some_file.raw")


def test_get_output_folder_rel_path_fallback() -> None:
    """Test that correct output folder is returned if no project_id is given."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id=None,
    )
    mock_raw_file.name = "some_file.raw"

    # when
    result = get_output_folder_rel_path(mock_raw_file, "some_fallback_id")

    assert result == Path("output/some_fallback_id/1970_01/out_some_file.raw")
