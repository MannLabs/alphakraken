"""Queries on the assignments of settings to projects."""

from shared.db._mongo import project_settings as _backend
from shared.domain.projects import ProjectSettings


def find(project_ids: list[str]) -> list[ProjectSettings]:
    """Return the settings assignments of the given projects with their settings, oldest first."""
    return _backend.find(project_ids)
