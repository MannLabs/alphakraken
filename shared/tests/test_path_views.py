"""Tests for the path_views module."""

from pathlib import Path, PurePosixPath, PureWindowsPath
from unittest.mock import patch

import pytest

from shared.keys import InternalPaths
from shared.path_views import (
    CONTAINER,
    Locations,
    View,
    get_cluster_view,
    get_host_view,
)
from shared.yamlsettings import YAMLSETTINGS, YamlKeys


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
    result = CONTAINER.resolve(Locations.BACKUP, "test1/1970_01/some_file.raw")

    assert result == Path("/opt/airflow/mounts/backup/test1/1970_01/some_file.raw")


def test_container_view_has_only_the_mounted_locations() -> None:
    """Test that the locations that are not mounted into the containers are absent."""
    assert [
        CONTAINER.has(location)
        for location in [Locations.INSTRUMENTS, Locations.BACKUP, Locations.OUTPUT]
    ] == [True, True, True]
    assert [
        CONTAINER.has(location)
        for location in [
            Locations.SETTINGS,
            Locations.SOFTWARE,
            Locations.SLURM,
            Locations.LOGS,
        ]
    ] == [False] * 4


def test_cluster_view() -> None:
    """Test that the cluster view is built from the absolute paths in the yaml settings."""
    locations = {
        YamlKeys.Locations.GENERAL: {YamlKeys.Locations.MOUNTS_PATH: "/some/mounts"},
        Locations.BACKUP: {YamlKeys.ABSOLUTE_PATH: "/some/pool/backup"},
        Locations.SETTINGS: {YamlKeys.ABSOLUTE_PATH: "/some/pool/settings"},
    }

    with patch.dict(YAMLSETTINGS, {YamlKeys.LOCATIONS: locations}):
        view = get_cluster_view()

    assert view.resolve(Locations.BACKUP, "test1/1970_01") == PurePosixPath(
        "/some/pool/backup/test1/1970_01"
    )
    assert view.resolve(Locations.SETTINGS) == PurePosixPath("/some/pool/settings")
    # the `general` section carries no absolute path and is not a location
    assert not view.has(YamlKeys.Locations.GENERAL)
    assert not view.has(Locations.OUTPUT)


def test_host_view() -> None:
    """Test that the host view mirrors the container view below the host mounts path."""
    locations = {
        YamlKeys.Locations.GENERAL: {YamlKeys.Locations.MOUNTS_PATH: "/some/mounts"}
    }

    with patch.dict(YAMLSETTINGS, {YamlKeys.LOCATIONS: locations}):
        view = get_host_view()

    assert view.resolve(Locations.OUTPUT, "P1/out_some_file.raw") == PurePosixPath(
        "/some/mounts/output/P1/out_some_file.raw"
    )
    assert not view.has(Locations.SETTINGS)


def test_host_view_raises_without_mounts_path() -> None:
    """Test that a missing mounts path in the yaml settings is reported."""
    with (
        patch.dict(YAMLSETTINGS, {YamlKeys.LOCATIONS: {}}),
        pytest.raises(KeyError, match="mounts_path"),
    ):
        get_host_view()


def test_locations_agree_with_the_container_and_yaml_key_names() -> None:
    """Test that the three vocabularies for the same folder names have not diverged."""
    assert (InternalPaths.INSTRUMENTS, InternalPaths.BACKUP, InternalPaths.OUTPUT) == (
        Locations.INSTRUMENTS,
        Locations.BACKUP,
        Locations.OUTPUT,
    )
    assert {
        YamlKeys.Locations.BACKUP,
        YamlKeys.Locations.SETTINGS,
        YamlKeys.Locations.OUTPUT,
        YamlKeys.Locations.SLURM,
        YamlKeys.Locations.SOFTWARE,
    } <= set(Locations.get_values())
