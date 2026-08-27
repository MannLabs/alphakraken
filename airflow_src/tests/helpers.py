"""Shared helpers for all tests."""

from collections.abc import Iterator
from contextlib import contextmanager
from unittest.mock import patch

from shared.yamlsettings import YAMLSETTINGS, YamlKeys


@contextmanager
def yaml_locations(**paths: str) -> Iterator[None]:
    """Override the `locations` section of the yaml settings, e.g. `yaml_locations(slurm="/path/to/slurm")`.

    Patches the settings that `get_path()` reads, rather than `get_path` at its import site,
    so tests stay valid when code moves between modules.
    """
    with patch.dict(
        YAMLSETTINGS,
        {
            YamlKeys.LOCATIONS: {
                key: {YamlKeys.ABSOLUTE_PATH: path} for key, path in paths.items()
            }
        },
    ):
        yield
