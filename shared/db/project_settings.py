"""Queries on the assignments of settings to projects."""

from shared.db._mongo import project_settings as _backend
from shared.domain.projects import ProjectSettings


def find(project_ids: list[str] | None = None) -> list[ProjectSettings]:
    """Return the settings assignments with their settings, oldest first.

    :param project_ids: only the assignments of these projects, None for all, also those of
        archived and deleted projects
    """
    return _backend.find(project_ids)
