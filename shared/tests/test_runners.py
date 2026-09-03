"""Tests for the runners module."""

from datetime import datetime
from pathlib import PurePosixPath, PureWindowsPath
from unittest.mock import MagicMock, patch

import pytest
import pytz

from shared.db.models import RawFile
from shared.keys import JobEngines
from shared.path_layout import get_output_folder_rel_path, get_raw_file_rel_path
from shared.path_views import Locations
from shared.runners import RUNNERS, OperatingSystems, _build_runners, get_runner
from shared.yamlsettings import YamlKeys

_SLURM_VIEW = {
    Locations.BACKUP: "/fs/pool/backup",
    Locations.OUTPUT: "/fs/pool/output",
    Locations.SETTINGS: "/fs/pool/settings",
    Locations.SOFTWARE: "/fs/home/software",
    Locations.SLURM: "/fs/pool/slurm",
}


def _entry(**overrides: object) -> dict:
    """Get a valid `slurm` runner entry, overridable per key."""
    entry = {
        YamlKeys.Runners.NAME: "slurm",
        YamlKeys.Runners.ENGINE: JobEngines.SLURM,
        YamlKeys.Runners.OS: OperatingSystems.LINUX,
        YamlKeys.Runners.SSH_CONNECTION_ID_PREFIX: "cluster_ssh_connection",
        YamlKeys.Runners.VIEW: _SLURM_VIEW,
    }
    entry.update(overrides)
    return {key: value for key, value in entry.items() if value is not ...}


def test_build_runners_keeps_the_yaml_order_and_fields() -> None:
    """Test that the runners are keyed by name in declaration order and carry their yaml fields."""
    entries = [
        _entry(),
        _entry(name="docker", engine=JobEngines.DOCKER, ssh_connection_id_prefix=...),
    ]

    # when
    runners = _build_runners(entries)

    assert list(runners) == ["slurm", "docker"]
    assert runners["slurm"].engine == JobEngines.SLURM
    assert runners["slurm"].os == OperatingSystems.LINUX
    assert runners["slurm"].ssh_connection_id_prefix == "cluster_ssh_connection"
    assert runners["slurm"].view.resolve(Locations.SLURM) == PurePosixPath(
        "/fs/pool/slurm"
    )
    assert runners["docker"].ssh_connection_id_prefix is None


@pytest.mark.parametrize("entries", [None, []])
def test_build_runners_rejects_missing_or_empty_list(entries: list | None) -> None:
    """Test that a yaml without runners fails naming the key."""
    with pytest.raises(ValueError, match="`runners`"):
        _build_runners(entries)


def test_build_runners_rejects_missing_name() -> None:
    """Test that a runner without a name is rejected."""
    with pytest.raises(KeyError, match="`name`"):
        _build_runners([_entry(name=...)])


def test_build_runners_rejects_duplicate_name() -> None:
    """Test that two runners with the same name are rejected, naming it."""
    with pytest.raises(ValueError, match="'slurm'.*`name`"):
        _build_runners([_entry(), _entry()])


def test_build_runners_rejects_unknown_engine() -> None:
    """Test that an engine outside `JobEngines` is rejected, naming runner and key."""
    with pytest.raises(ValueError, match="'slurm'.*`engine`.*'kubernetes'"):
        _build_runners([_entry(engine="kubernetes")])


def test_build_runners_rejects_missing_os() -> None:
    """Test that the os is required."""
    with pytest.raises(KeyError, match="'slurm'.*`os`"):
        _build_runners([_entry(os=...)])


def test_build_runners_rejects_unknown_os() -> None:
    """Test that an os outside `OperatingSystems` is rejected, naming runner and key."""
    with pytest.raises(ValueError, match="'slurm'.*`os`.*'plan9'"):
        _build_runners([_entry(os="plan9")])


def test_build_runners_rejects_missing_view() -> None:
    """Test that the view is required."""
    with pytest.raises(KeyError, match="'slurm'.*`view`"):
        _build_runners([_entry(view=...)])


def test_build_runners_rejects_unknown_view_key() -> None:
    """Test that a view key outside `Locations` is rejected, naming runner and key."""
    with pytest.raises(ValueError, match="'slurm'.*`view`.*'scratch'"):
        _build_runners([_entry(view={**_SLURM_VIEW, "scratch": "/scratch"})])


def test_build_runners_accepts_prefix_on_docker_runner() -> None:
    """Test that the prefix is not interpreted: the loader knows nothing about which engines use SSH."""
    runners = _build_runners([_entry(name="docker", engine=JobEngines.DOCKER)])

    assert runners["docker"].ssh_connection_id_prefix == "cluster_ssh_connection"


def test_build_runners_accepts_runner_without_slurm_location() -> None:
    """Test that a missing location is not an error at load time, only at first use."""
    view = {key: value for key, value in _SLURM_VIEW.items() if key != Locations.SLURM}

    runners = _build_runners([_entry(view=view)])

    assert not runners["slurm"].view.has(Locations.SLURM)
    with pytest.raises(KeyError, match="'slurm' is not reachable in the 'slurm' view"):
        runners["slurm"].view.resolve(Locations.SLURM)


def test_build_runners_treats_macos_like_linux() -> None:
    """Test that a macos runner gets the posix path flavour."""
    runners = _build_runners([_entry(), _entry(name="mac", os=OperatingSystems.MACOS)])

    assert runners["mac"].view.resolve(Locations.BACKUP) == runners[
        "slurm"
    ].view.resolve(Locations.BACKUP)
    assert isinstance(runners["mac"].view.resolve(Locations.BACKUP), PurePosixPath)


def test_build_runners_windows_view_resolves_layout_paths() -> None:
    """Test that a windows runner turns the posix layout paths into UNC and drive letter paths."""
    runners = _build_runners(
        [
            _entry(
                name="win_box",
                os=OperatingSystems.WINDOWS,
                view={
                    Locations.BACKUP: r"\\server\share\backup",
                    Locations.OUTPUT: r"Z:\alphakraken\output",
                },
            )
        ]
    )
    raw_file = MagicMock(
        wraps=RawFile,
        id="f.raw",
        instrument_id="test1",
        project_id="P1",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
    )
    raw_file.has_project = True

    # when
    view = runners["win_box"].view
    raw_file_path = view.resolve(Locations.BACKUP, get_raw_file_rel_path(raw_file))
    output_path = view.resolve(
        Locations.OUTPUT, get_output_folder_rel_path(raw_file, "alphadia")
    )

    assert isinstance(raw_file_path, PureWindowsPath)
    assert str(raw_file_path) == r"\\server\share\backup\test1\1970_01\f.raw"
    assert str(output_path) == r"Z:\alphakraken\output\P1\out_f.raw\alphadia"


def test_runners_are_built_from_the_yaml_at_import() -> None:
    """Test that the `_test_` stub yields the three runners the tests rely on."""
    assert list(RUNNERS) == [
        JobEngines.SLURM,
        JobEngines.DOCKER,
        JobEngines.FILE_BASED,
    ]


def test_get_runner() -> None:
    """Test that a declared runner is returned by name."""
    assert get_runner(JobEngines.DOCKER) is RUNNERS[JobEngines.DOCKER]


def test_get_runner_raises_listing_the_known_names() -> None:
    """Test that an undeclared runner is reported together with the declared ones."""
    with (
        patch.dict(RUNNERS, {"a": MagicMock(), "b": MagicMock()}, clear=True),
        pytest.raises(KeyError, match=r"'nope'.*\['a', 'b'\]"),
    ):
        get_runner("nope")
