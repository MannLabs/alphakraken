"""Convert an alphakraken.<env>.yaml from the pre-runner format to the `mounts` + `runners` format.

The `locations` block is dissolved:
    locations.<loc>.mount_src/username   -> mounts.<mount_target>          (mount information)
    locations.<loc>.absolute_path        -> runners[0].view.<loc>          (paths as seen by the runner)
    locations.{backup,output}.absolute_path -> display_paths.{backup,output} (display paths in the webapp)
    locations.general.mounts_path        -> `MOUNTS_PATH` in envs/<env>.env, reported for comparison
    instruments.<id>.mount_target        -> dropped, the target is `instruments/<id>`

Each input is written next to it as `<name>.converted`, comments are lost: diff the two files and
re-add them.

One runner named `slurm` is emitted, because the whole `locations` block described one set of paths.
A deployment whose settings entries use another engine needs a runner named after that engine, cf.
`_ENGINE_TO_RUNNER` in `_migrate_job_engine_to_runner.py`: copy the block and adapt `engine`.

# Usage:
    PYTHONPATH=. python shared/_migrations/from_0.10.0/_convert_alphakraken_yaml.py envs/alphakraken.*.yaml
"""

import argparse
import logging
from pathlib import Path
from typing import Any

import yaml

from shared.keys import JobEngines
from shared.path_views import Locations

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# the locations shown to users, cf. `_REQUIRED_LOCATIONS` in `shared.display_paths`
_DISPLAY_LOCATIONS = (
    Locations.BACKUP,
    Locations.OUTPUT,
    Locations.SETTINGS,
    Locations.SOFTWARE,
)

# the locations a slurm runner needs, cf. `_REQUIRED_LOCATIONS` in `shared.runners`
_VIEW_LOCATIONS = (
    Locations.BACKUP,
    Locations.OUTPUT,
    Locations.SETTINGS,
    Locations.SOFTWARE,
)

_RUNNER_NAME = JobEngines.SLURM
_RUNNER_OS = "linux"  # `shared.runners.OperatingSystems`, not imported: it reads the yaml at import
_SSH_CONNECTION_ID_PREFIX = "cluster_ssh_connection"

_CONVERTED_SUFFIX = ".converted"


def _build_mounts(locations: dict[str, Any]) -> dict[str, Any]:
    """Get the `mounts` block: one entry per location that is mounted, keyed by its mount target."""
    mounts = {}
    for name, values in locations.items():
        if not isinstance(values, dict) or "mount_src" not in values:
            continue

        target = values.get("mount_target", name)
        mounts[target] = {
            key: values[key] for key in ("username", "mount_src") if key in values
        }

    return mounts


def _build_view(locations: dict[str, Any]) -> dict[str, str]:
    """Get the runner's `view`: the absolute path of each location on the shared file system."""
    view = {}
    for location in _VIEW_LOCATIONS:
        if (absolute_path := locations.get(location, {}).get("absolute_path")) is None:
            raise ValueError(f"Key `locations.{location}.absolute_path` is missing.")
        view[location] = absolute_path

    return view


def _strip_mount_targets(instruments: dict[str, Any]) -> dict[str, Any]:
    """Drop `mount_target` from each instrument, after checking it is the one the new format implies."""
    stripped = {}
    for instrument_id, values in instruments.items():
        expected = f"{Locations.INSTRUMENTS}/{instrument_id}"
        if (target := values.get("mount_target", expected)) != expected:
            raise ValueError(
                f"Instrument '{instrument_id}' mounts at '{target}', but the new format always "
                f"mounts at '{expected}'. Rename the folder or the instrument first."
            )
        stripped[instrument_id] = {
            key: value for key, value in values.items() if key != "mount_target"
        }

    return stripped


def convert(config: dict[str, Any]) -> dict[str, Any]:
    """Convert a parsed alphakraken.yaml to the `mounts` + `runners` format."""
    if "locations" not in config:
        raise ValueError("No `locations` block, this file is already converted.")

    locations = config["locations"]
    view = _build_view(locations)

    return {
        "instruments": _strip_mount_targets(config["instruments"]),
        "mounts": _build_mounts(locations),
        "general": config["general"],
        "display_paths": {location: view[location] for location in _DISPLAY_LOCATIONS},
        "backup": config["backup"],
        "runners": [
            {
                "name": _RUNNER_NAME,
                "engine": JobEngines.SLURM,
                "os": _RUNNER_OS,
                "ssh_connection_id_prefix": _SSH_CONNECTION_ID_PREFIX,
                "view": view,
            }
        ],
    }


def convert_file(file_path: Path) -> Path:
    """Convert the alphakraken.yaml at `file_path`, writing the result next to it."""
    config = yaml.safe_load(file_path.read_text())
    converted = convert(config)

    if mounts_path := config["locations"].get("general", {}).get("mounts_path"):
        env_name = file_path.stem.removeprefix("alphakraken.")
        logger.info(
            f"Set `MOUNTS_PATH={mounts_path}` in envs/{env_name}.env, it must be an absolute path."
        )

    target_path = file_path.with_name(file_path.name + _CONVERTED_SUFFIX)
    target_path.write_text(
        yaml.safe_dump(converted, sort_keys=False, default_flow_style=False)
    )
    return target_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Convert alphakraken.<env>.yaml files to the `mounts` + `runners` format."
    )
    parser.add_argument("file_paths", nargs="+", type=Path, help="the yaml files")
    args = parser.parse_args()

    for path in args.file_paths:
        logger.info(f"{path} -> {convert_file(path)}")
