"""The data directories as users see them on the shared file system.

Only for display (webapp, REST API): the DB holds location-relative paths, the pipeline uses the
runner views.
"""

from typing import Any

from shared.db.models import RawFile
from shared.path_layout import get_raw_file_folder_rel_path
from shared.path_views import Locations
from shared.yamlsettings import YAMLSETTINGS, YamlKeys

_REQUIRED_LOCATIONS = (Locations.BACKUP, Locations.OUTPUT)

_POSIX_SEPARATOR = "/"
_WINDOWS_SEPARATOR = "\\"


def _build_display_paths(settings: dict[str, Any]) -> dict[str, str]:
    """Get the base path of each location from the yaml settings, failing on missing locations."""
    paths = settings.get(YamlKeys.DISPLAY_PATHS) or {}
    if missing := [
        location for location in _REQUIRED_LOCATIONS if location not in paths
    ]:
        raise KeyError(
            f"Key `{YamlKeys.DISPLAY_PATHS}` in alphakraken.yaml lacks {missing}."
        )
    return paths


DISPLAY_PATHS: dict[str, str] = _build_display_paths(YAMLSETTINGS)


def _display_path(location: str, rel_path: str) -> str:
    r"""Put `rel_path` below the display base of `location`, in the separator the base uses.

    Strings rather than paths: a display path is never resolved, and the base may be a windows
    path (e.g. `\\server\share\backup`), which posix semantics would join with the wrong
    separator.
    """
    base = DISPLAY_PATHS[location]
    separator = _WINDOWS_SEPARATOR if _WINDOWS_SEPARATOR in base else _POSIX_SEPARATOR

    return (
        f"{base.rstrip(separator)}{separator}"
        f"{rel_path.replace(_POSIX_SEPARATOR, separator)}"
    )


def get_display_backup_path(raw_file: RawFile) -> str:
    """Get the folder holding the backup of `raw_file`, as users see it."""
    return _display_path(Locations.BACKUP, str(get_raw_file_folder_rel_path(raw_file)))


def get_display_output_path(relative_output_path: str) -> str:
    """Get the output folder for `relative_output_path`, as users see it."""
    return _display_path(Locations.OUTPUT, relative_output_path)
