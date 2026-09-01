"""Shared helpers for all tests."""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import PurePosixPath
from unittest.mock import patch

from shared.path_views import CLUSTER_VIEW
from shared.yamlsettings import YAMLSETTINGS, YamlKeys


@contextmanager
def yaml_locations(**paths: str) -> Iterator[None]:
    """Override the `locations` section of the yaml settings, e.g. `yaml_locations(slurm="/path/to/slurm")`.

    Patches the settings that `get_path()` reads and the contents of `CLUSTER_VIEW`, rather than
    either of them at its import site, so tests stay valid when code moves between modules.
    """
    with (
        patch.dict(
            YAMLSETTINGS,
            {
                YamlKeys.LOCATIONS: {
                    key: {YamlKeys.ABSOLUTE_PATH: path} for key, path in paths.items()
                }
            },
        ),
        patch.object(
            CLUSTER_VIEW,
            "_locations",
            {key: PurePosixPath(path) for key, path in paths.items()},
        ),
    ):
        yield
