"""Tests that the deployment configuration agrees with the path views.

`docker-compose.yaml` and the `envs/alphakraken.*.yaml` files decide where the data actually
shows up in the containers and on the docker host. Nothing but convention (the "DO NOT CHANGE"
comments) keeps them in sync with `path_views`, hence these tests.
"""

import re
from pathlib import Path

import pytest
import yaml

from shared.keys import InternalPaths, JobEngines
from shared.path_views import AIRFLOW_CONTAINER_VIEW, Locations
from shared.runners import _build_runners
from shared.yamlsettings import YamlKeys

_REPO_ROOT = Path(__file__).parents[2]

# "<host path>:<container path>[:<mode>]", where the host path holds a variable with a colon in it
_BIND = re.compile(r"^(?P<host>.+?):(?P<container>/opt/airflow/[^:]+)(?::[a-z]+)?$")

# the logs are mounted outside the mounts folder and under a different name, cf. `mount.sh`
_LOGS_MOUNT_TARGET = "airflow_logs"
_LOGS_CONTAINER_PATH = "/opt/airflow/logs"

# host side of every bind below the mounts folder, cf. `docker-compose.yaml`
_HOST_MOUNTS_PREFIX = "${MOUNTS_PATH:?error}/"


def _mount_binds() -> list[tuple[str, str]]:
    """Get all (host path, container path) binds of `docker-compose.yaml` below the mounts folder."""
    compose = yaml.safe_load((_REPO_ROOT / "docker-compose.yaml").read_text())

    binds = []
    for service in compose["services"].values():
        for volume in service.get("volumes") or []:
            match = _BIND.match(str(volume))
            if match and match["container"].startswith(InternalPaths.MOUNTS_PATH):
                binds.append((match["host"], match["container"]))

    return binds


def _env_yamls() -> list[tuple[str, dict]]:
    """Get the name and content of each environment configuration."""
    return [
        (path.name, yaml.safe_load(path.read_text()))
        for path in sorted((_REPO_ROOT / "envs").glob("alphakraken.*.yaml"))
    ]


def test_compose_binds_mirror_the_mounts_folder() -> None:
    """Test that each bind puts the data at the same path below the mounts folder on both sides.

    This is what lets the docker host view be derived from `MOUNTS_PATH`.
    """
    for host_path, container_path in _mount_binds():
        rel_path = container_path.removeprefix(InternalPaths.MOUNTS_PATH)

        assert host_path.endswith(f"/{rel_path}"), (
            f"bind '{host_path}:{container_path}' does not mirror '{rel_path}'"
        )


def test_compose_binds_are_in_a_known_location() -> None:
    """Test that nothing is mounted into the containers that the container view does not know."""
    for _, container_path in _mount_binds():
        location = container_path.removeprefix(InternalPaths.MOUNTS_PATH).split("/")[0]

        assert AIRFLOW_CONTAINER_VIEW.has(location), (
            f"'{container_path}' is mounted but '{location}' is no location of the view"
        )


def test_every_container_location_is_mounted() -> None:
    """Test that each location of the container view is backed by a bind."""
    mounted = {
        container_path.removeprefix(InternalPaths.MOUNTS_PATH).split("/")[0]
        for _, container_path in _mount_binds()
    }

    for location in [Locations.INSTRUMENTS, Locations.BACKUP, Locations.OUTPUT]:
        assert location in mounted, f"no bind for location '{location}'"


@pytest.mark.parametrize(("file_name", "config"), _env_yamls())
def test_no_top_level_locations_key(file_name: str, config: dict) -> None:
    """Test that the pre-runner `locations` block is gone: its paths live in `runners` and `mounts`."""
    assert "locations" not in config, file_name


@pytest.mark.parametrize(("file_name", "config"), _env_yamls())
def test_every_mount_has_a_source(file_name: str, config: dict) -> None:
    """Test that each mount entry carries what mount.sh reads; the target is the entry's name."""
    for name, values in config[YamlKeys.MOUNTS].items():
        assert "mount_src" in values, f"{file_name}: mount '{name}' has no mount_src"


def _host_mount_folders() -> set[str]:
    """Get the folders below the mounts folder that `docker-compose.yaml` binds into a container."""
    compose = yaml.safe_load((_REPO_ROOT / "docker-compose.yaml").read_text())

    return {
        str(volume).removeprefix(_HOST_MOUNTS_PREFIX).split(":")[0].split("/")[0]
        for service in compose["services"].values()
        for volume in service.get("volumes") or []
        if str(volume).startswith(_HOST_MOUNTS_PREFIX)
    }


@pytest.mark.parametrize(("file_name", "config"), _env_yamls())
def test_every_mount_is_bound_into_a_container(file_name: str, config: dict) -> None:
    """Test that mount.sh mounts each entry at a folder some container reads, i.e. the name is right."""
    for name in config[YamlKeys.MOUNTS]:
        assert name in _host_mount_folders(), (
            f"{file_name}: nothing binds '{name}' below the mounts folder"
        )


@pytest.mark.parametrize(("file_name", "config"), _env_yamls())
def test_backup_base_path_is_declared(file_name: str, config: dict) -> None:
    """Test that each environment declares the folder the backups are displayed under."""
    assert config["backup"]["backup_base_path"], file_name


@pytest.mark.parametrize(("file_name", "config"), _env_yamls())
def test_runners_are_valid(file_name: str, config: dict) -> None:
    """Test that each environment declares at least one runner and all pass the validation."""
    assert _build_runners(config[YamlKeys.RUNNERS]), file_name


@pytest.mark.parametrize(("file_name", "config"), _env_yamls())
def test_slurm_runners_declare_every_location_they_use(
    file_name: str, config: dict
) -> None:
    """Test that a slurm runner reaches all five locations, which the loader does not check."""
    runners = _build_runners(config[YamlKeys.RUNNERS])

    for runner in runners.values():
        if runner.engine != JobEngines.SLURM:
            continue
        for location in [
            Locations.BACKUP,
            Locations.OUTPUT,
            Locations.SETTINGS,
            Locations.SOFTWARE,
            Locations.SLURM,
        ]:
            assert runner.view.has(location), (
                f"{file_name}: runner '{runner.name}' lacks '{location}'"
            )


@pytest.mark.parametrize(("file_name", "config"), _env_yamls())
def test_backup_base_path_equals_the_slurm_backup_location(
    file_name: str, config: dict
) -> None:
    """Test that the persisted display path is the path the slurm runner sees."""
    runners = _build_runners(config[YamlKeys.RUNNERS])

    assert str(runners[JobEngines.SLURM].view.resolve(Locations.BACKUP)) == str(
        config["backup"]["backup_base_path"]
    ), file_name


def test_the_logs_are_not_mounted_below_the_mounts_folder() -> None:
    """Test the one bind that deliberately breaks the mirroring, so that it stays deliberate."""
    compose = yaml.safe_load((_REPO_ROOT / "docker-compose.yaml").read_text())

    log_binds = {
        str(volume)
        for service in compose["services"].values()
        for volume in service.get("volumes") or []
        if _LOGS_CONTAINER_PATH in str(volume)
    }

    assert log_binds == {
        f"${{MOUNTS_PATH:?error}}/{_LOGS_MOUNT_TARGET}:{_LOGS_CONTAINER_PATH}:rw"
    }
