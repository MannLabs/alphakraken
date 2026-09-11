"""Tests for the path_views module."""

import os
from pathlib import Path, PurePosixPath, PureWindowsPath
from unittest.mock import patch

import pytest

from shared.keys import EnvVars
from shared.path_views import (
    AIRFLOW_CONTAINER_VIEW,
    Locations,
    View,
    _build_docker_host_view,
)


def test_resolve() -> None:
    """Test that a relative path is resolved against the location it belongs to."""
    view = View("some_view", {Locations.BACKUP: "/some/backup"}, PurePosixPath)

    # when
    result = view.resolve(Locations.BACKUP, Path("test1/1970_01/some_file.raw"))

    assert result == PurePosixPath("/some/backup/test1/1970_01/some_file.raw")


def test_resolve_without_rel_path() -> None:
    """Test that omitting the relative path yields the location itself."""
    view = View("some_view", {Locations.BACKUP: "/some/backup"}, PurePosixPath)

    assert view.resolve(Locations.BACKUP) == PurePosixPath("/some/backup")


def test_resolve_raises_on_unreachable_location() -> None:
    """Test that resolving a location that the view does not have names both view and location."""
    view = View("some_view", {Locations.BACKUP: "/some/backup"}, PurePosixPath)

    with pytest.raises(
        KeyError, match="'settings' is not reachable in the 'some_view' view"
    ):
        view.resolve(Locations.SETTINGS)


def test_has() -> None:
    """Test that a view reports which locations it can reach."""
    view = View("some_view", {Locations.BACKUP: "/some/backup"}, PurePosixPath)

    assert view.has(Locations.BACKUP)
    assert not view.has(Locations.SETTINGS)


def test_resolve_windows_unc_path() -> None:
    """Test that a windows view resolves a relative path to a UNC path."""
    view = View(
        "some_view", {Locations.BACKUP: "//some_server/some_share"}, PureWindowsPath
    )

    # when
    result = view.resolve(
        Locations.BACKUP, PurePosixPath("test1/1970_01/some_file.raw")
    )

    assert str(result) == r"\\some_server\some_share\test1\1970_01\some_file.raw"


def test_resolve_windows_drive_letter_path() -> None:
    """Test that a windows view resolves a relative path to a mapped drive path."""
    view = View("some_view", {Locations.BACKUP: "Z:/backup"}, PureWindowsPath)

    # when
    result = view.resolve(
        Locations.BACKUP, PurePosixPath("test1/1970_01/some_file.raw")
    )

    assert str(result) == r"Z:\backup\test1\1970_01\some_file.raw"


def test_container_view() -> None:
    """Test that the container view resolves to the mounts folder, as a local path."""
    result = AIRFLOW_CONTAINER_VIEW.resolve(
        Locations.BACKUP, "test1/1970_01/some_file.raw"
    )

    assert result == Path("/opt/airflow/mounts/backup/test1/1970_01/some_file.raw")


def test_container_view_has_only_the_mounted_locations() -> None:
    """Test that the locations that are not mounted into the containers are absent."""
    assert [
        AIRFLOW_CONTAINER_VIEW.has(location)
        for location in [Locations.INSTRUMENTS, Locations.BACKUP, Locations.OUTPUT]
    ] == [True, True, True]
    assert [
        AIRFLOW_CONTAINER_VIEW.has(location)
        for location in [
            Locations.SETTINGS,
            Locations.SOFTWARE,
            Locations.SLURM,
            Locations.LOGS,
        ]
    ] == [False] * 4


def test_docker_host_view() -> None:
    """Test that the docker host view mirrors the container view below the host mounts path."""
    with patch.dict(os.environ, {EnvVars.MOUNTS_PATH: "/some/mounts"}):
        view = _build_docker_host_view()

    assert view.resolve(Locations.OUTPUT, "P1/out_some_file.raw") == PurePosixPath(
        "/some/mounts/output/P1/out_some_file.raw"
    )
    assert not view.has(Locations.SETTINGS)


def test_docker_host_view_without_mounts_env_reaches_nothing() -> None:
    """Test that an unset `MOUNTS_PATH` yields an empty view, reported only when it is used."""
    with patch.dict(os.environ):
        del os.environ[EnvVars.MOUNTS_PATH]
        view = _build_docker_host_view()

    assert not view.has(Locations.OUTPUT)
    with pytest.raises(KeyError, match="not reachable in the 'docker host' view"):
        view.resolve(Locations.OUTPUT)
