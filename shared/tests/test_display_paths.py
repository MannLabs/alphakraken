"""Tests for the display paths."""

from datetime import datetime

import pytest

from shared.db.models import RawFile
from shared.display_paths import (
    DISPLAY_PATHS,
    _build_display_paths,
    _display_path,
    get_display_backup_path,
    get_display_output_path,
)


def test_build_display_paths_returns_all_locations() -> None:
    """Test that the base paths are taken from the `display_paths` block."""
    paths = {
        "backup": "/fs/backup",
        "output": "/fs/output",
        "settings": "/fs/settings",
        "software": "/fs/software",
    }

    assert _build_display_paths({"display_paths": paths}) == paths


@pytest.mark.parametrize(
    "settings",
    [{}, {"display_paths": None}, {"display_paths": {"backup": "/fs/backup"}}],
)
def test_build_display_paths_raises_naming_the_key(settings: dict) -> None:
    """Test that a missing location is reported with the yaml key."""
    with pytest.raises(KeyError, match="display_paths"):
        _build_display_paths(settings)


@pytest.mark.parametrize(
    ("base", "expected"),
    [
        ("/fs/backup", "/fs/backup/test1/2024_07"),
        ("/fs/backup/", "/fs/backup/test1/2024_07"),
        (r"\\server\share\backup", r"\\server\share\backup\test1\2024_07"),
        (r"Z:\alphakraken\backup", r"Z:\alphakraken\backup\test1\2024_07"),
    ],
)
def test_display_path_keeps_the_separator_of_the_base(
    base: str, expected: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that a windows base yields a windows path, not a mix of both separators."""
    monkeypatch.setitem(DISPLAY_PATHS, "backup", base)

    assert _display_path("backup", "test1/2024_07") == expected


def test_get_display_backup_path() -> None:
    """Test that the backup folder follows the path layout below the display backup path."""
    raw_file = RawFile(
        id="f.raw",
        instrument_id="test1",
        created_at=datetime(2024, 7, 1),  # noqa: DTZ001
    )

    assert get_display_backup_path(raw_file) == "./tmp/test/backup/test1/2024_07"


def test_get_display_output_path() -> None:
    """Test that the relative output path is placed below the display output path."""
    assert (
        get_display_output_path("P1/out_f.raw/alphadia")
        == "./tmp/test/output/P1/out_f.raw/alphadia"
    )
