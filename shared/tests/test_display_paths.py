"""Tests for the display paths."""

from datetime import datetime
from pathlib import PurePosixPath

import pytest

from shared.db.models import RawFile
from shared.display_paths import (
    _build_display_view,
    get_display_backup_folder,
    get_display_output_path,
)


def test_build_display_view_resolves_both_locations() -> None:
    """Test that the view is built from the `display_paths` block with posix paths."""
    view = _build_display_view(
        {"display_paths": {"backup": "/fs/backup", "output": "/fs/output"}}
    )

    assert view.resolve("backup", "a/b") == PurePosixPath("/fs/backup/a/b")
    assert view.resolve("output") == PurePosixPath("/fs/output")


@pytest.mark.parametrize(
    "settings",
    [{}, {"display_paths": None}, {"display_paths": {"backup": "/fs/backup"}}],
)
def test_build_display_view_raises_naming_the_key(settings: dict) -> None:
    """Test that a missing location is reported with the yaml key."""
    with pytest.raises(KeyError, match="display_paths"):
        _build_display_view(settings)


def test_get_display_backup_folder() -> None:
    """Test that the backup folder follows the path layout below the display backup path."""
    raw_file = RawFile(
        id="f.raw",
        instrument_id="test1",
        created_at=datetime(2024, 7, 1),  # noqa: DTZ001
    )

    assert get_display_backup_folder(raw_file) == PurePosixPath(
        "tmp/test/backup/test1/2024_07"
    )


def test_get_display_output_path() -> None:
    """Test that the relative output path is placed below the display output path."""
    assert get_display_output_path("P1/out_f.raw/alphadia") == PurePosixPath(
        "tmp/test/output/P1/out_f.raw/alphadia"
    )
