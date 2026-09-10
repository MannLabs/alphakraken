"""Tests for the alphakraken.yaml pre-runner -> `mounts` + `runners` converter."""

import importlib.util
from pathlib import Path
from typing import Any

import pytest

_SCRIPT = (
    Path(__file__).parents[1] / "_migrations/from_0.10.0/_convert_alphakraken_yaml.py"
)

# the migrations folder is no package, so the script is loaded by path
_spec = importlib.util.spec_from_file_location("convert_alphakraken_yaml", _SCRIPT)
converter: Any = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(converter)  # type: ignore[union-attr]


def _config(**overrides: Any) -> dict:
    """Get a valid pre-runner config, overridable per top-level key."""
    config = {
        "instruments": {
            "test1": {"type": "thermo", "mount_src": "//x/test1"},
            "test2": {"type": "sciex", "mount_target": "instruments/test2"},
        },
        "locations": {
            "general": {"mounts_path": "/host/mounts"},
            "backup": {
                "username": "user",
                "mount_src": "//x/backup",
                "mount_target": "backup",
                "absolute_path": "/fs/backup",
            },
            "output": {
                "username": "user",
                "mount_src": "//x/output",
                "mount_target": "output",
                "absolute_path": "/fs/output",
            },
            "logs": {
                "username": "user",
                "mount_src": "//x/airflow_logs",
                "mount_target": "airflow_logs",
            },
            "settings": {"absolute_path": "/fs/settings"},
            "software": {"absolute_path": "/fs/software"},
        },
        "general": {"notifications": {"webapp_url": "http://localhost:8501"}},
        "backup": {"backup_type": "local"},
    }
    config.update(overrides)
    return config


def test_converts_a_full_config() -> None:
    """Test that the locations block ends up in `mounts`, the runner's `view` and `backup`."""
    # when
    converted = converter.convert(_config())

    assert converted["mounts"] == {
        "backup": {"username": "user", "mount_src": "//x/backup"},
        "output": {"username": "user", "mount_src": "//x/output"},
        "airflow_logs": {"username": "user", "mount_src": "//x/airflow_logs"},
    }
    assert converted["runners"] == [
        {
            "name": "slurm",
            "engine": "slurm",
            "os": "linux",
            "ssh_connection_id_prefix": "cluster_ssh_connection",
            "view": {
                "backup": "/fs/backup",
                "output": "/fs/output",
                "settings": "/fs/settings",
                "software": "/fs/software",
            },
        }
    ]
    assert converted["display_paths"] == {
        "backup": "/fs/backup",
        "output": "/fs/output",
    }
    assert converted["backup"] == {"backup_type": "local"}
    assert converted["general"] == _config()["general"]
    assert "locations" not in converted


def test_drops_the_instrument_mount_targets() -> None:
    """Test that the implied instrument mount target is removed, the rest of the entry kept."""
    # when
    converted = converter.convert(_config())

    assert converted["instruments"] == {
        "test1": {"type": "thermo", "mount_src": "//x/test1"},
        "test2": {"type": "sciex"},
    }


def test_rejects_an_instrument_mounted_elsewhere() -> None:
    """Test that a mount target the new format cannot express stops the conversion."""
    instruments = {"test1": {"type": "thermo", "mount_target": "elsewhere/test1"}}

    with pytest.raises(ValueError, match="mounts at 'elsewhere/test1'"):
        converter.convert(_config(instruments=instruments))


def test_rejects_a_missing_absolute_path() -> None:
    """Test that a location the runner needs but the config lacks stops the conversion."""
    locations = {
        key: value for key, value in _config()["locations"].items() if key != "software"
    }

    with pytest.raises(ValueError, match="locations.software.absolute_path"):
        converter.convert(_config(locations=locations))


def test_rejects_an_already_converted_config() -> None:
    """Test that a config without a locations block is not converted twice."""
    config = {key: value for key, value in _config().items() if key != "locations"}

    with pytest.raises(ValueError, match="already converted"):
        converter.convert(config)
