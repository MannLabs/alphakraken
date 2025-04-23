"""Tests for the paths module."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytz
from common.paths import get_output_folder_rel_path

from shared.db.models import RawFile


def test_get_output_folder_rel_path_no_fallback() -> None:
    """Test that correct output folder is returned if project_id is given."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="some_file.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id="some_project_id",
    )

    # when
    result = get_output_folder_rel_path(mock_raw_file, "some_project_id")

    assert result == Path("some_project_id/out_some_file.raw")


def test_get_output_folder_rel_path_fallback() -> None:
    """Test that correct output folder is returned if no project_id is given."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="some_file.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id=None,
    )

    # when
    result = get_output_folder_rel_path(mock_raw_file, "some_fallback_id")

    assert result == Path("some_fallback_id/1970_01/out_some_file.raw")
