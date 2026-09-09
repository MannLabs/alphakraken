"""The data directories as users see them on the shared file system.

Only for display (webapp, REST API): the DB holds location-relative paths, the pipeline uses the
runner views.
"""

from pathlib import PurePosixPath
from typing import Any

from shared.db.models import RawFile
from shared.path_layout import get_raw_file_folder_rel_path
from shared.path_views import Locations, View
from shared.yamlsettings import YAMLSETTINGS, YamlKeys

_REQUIRED_LOCATIONS = (Locations.BACKUP, Locations.OUTPUT)


def _build_display_view(settings: dict[str, Any]) -> View[PurePosixPath]:
    """Build the display view from the yaml settings, failing on missing locations."""
    paths = settings.get(YamlKeys.DISPLAY_PATHS) or {}
    if missing := [
        location for location in _REQUIRED_LOCATIONS if location not in paths
    ]:
        raise KeyError(
            f"Key `{YamlKeys.DISPLAY_PATHS}` in alphakraken.yaml lacks {missing}."
        )
    return View("display", paths, PurePosixPath)


DISPLAY_VIEW: View[PurePosixPath] = _build_display_view(YAMLSETTINGS)


def get_display_backup_folder(raw_file: RawFile) -> PurePosixPath:
    """Get the folder holding the backup of `raw_file`, as users see it."""
    return DISPLAY_VIEW.resolve(
        Locations.BACKUP, get_raw_file_folder_rel_path(raw_file)
    )


def get_display_output_path(relative_output_path: str) -> PurePosixPath:
    """Get the output folder for `relative_output_path`, as users see it."""
    return DISPLAY_VIEW.resolve(Locations.OUTPUT, relative_output_path)
