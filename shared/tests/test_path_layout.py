"""Tests for the path_layout module."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytz

from shared.db.models import RawFile
from shared.path_layout import (
    get_output_folder_rel_path,
    get_raw_file_folder_rel_path,
    get_raw_file_rel_path,
)


def test_get_output_folder_rel_path_no_fallback() -> None:
    """Test that correct output folder is returned if project_id is given."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="some_file.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id="some_project_id",
    )

    # when
    result = get_output_folder_rel_path(mock_raw_file)

    assert result == Path("some_project_id/out_some_file.raw")


def test_get_output_folder_rel_path_fallback() -> None:
    """Test that correct output folder is returned if no project_id is given."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="some_file.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id="_FALLBACK",
        has_project=False,
    )

    # when
    result = get_output_folder_rel_path(mock_raw_file)

    assert result == Path("_FALLBACK/1970_01/out_some_file.raw")


def test_get_output_folder_rel_path_with_software_type() -> None:
    """Test that software_type is appended as subfolder."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="some_file.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        project_id="some_project_id",
    )

    result = get_output_folder_rel_path(mock_raw_file, software_type="alphadia")

    assert result == Path("some_project_id/out_some_file.raw/alphadia")


def test_get_raw_file_folder_rel_path() -> None:
    """Test that the raw file folder is instrument and creation month."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="some_file.raw",
        instrument_id="test1",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
    )

    # when
    result = get_raw_file_folder_rel_path(mock_raw_file)

    assert result == Path("test1/1970_01")


def test_get_raw_file_rel_path() -> None:
    """Test that the raw file path is the raw file folder plus the raw file id."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="some_file.raw",
        instrument_id="test1",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
    )

    # when
    result = get_raw_file_rel_path(mock_raw_file)

    assert result == Path("test1/1970_01/some_file.raw")
