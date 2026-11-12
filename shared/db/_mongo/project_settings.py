"""Queries on the assignments of settings to projects on MongoDB."""

from shared.db import models
from shared.db._mongo.connection import ensure_connected
from shared.db._mongo.errors import translate_errors
from shared.db._mongo.mappers import to_project_settings, to_settings
from shared.db._mongo.settings import settings_by_unique_name
from shared.domain.projects import ProjectSettings


@translate_errors
def find(project_ids: list[str] | None) -> list[ProjectSettings]:
    """Return the settings assignments of the given projects, cf. `shared.db.project_settings`."""
    ensure_connected()
    conditions = {} if project_ids is None else {"project__in": project_ids}
    docs = list(models.ProjectSettings.objects(**conditions).order_by("created_at_"))
    # one query for all settings, where dereferencing would run one per assignment
    settings_ids = {doc.to_mongo()["settings"] for doc in docs}
    settings_by_id = {
        settings_doc.pk: to_settings(settings_doc)
        for settings_doc in models.Settings.objects(id__in=settings_ids)
    }
    return [
        to_project_settings(doc, settings_by_id[doc.to_mongo()["settings"]])
        for doc in docs
    ]


@translate_errors
def assign(  # noqa: PLR0913
    project_id: str,
    settings_id: str,
    *,
    scopes: list[str],
    excluded_scopes: list[str],
    raw_file_id_filter: list[str],
    raw_file_id_exclude_filter: list[str],
) -> ProjectSettings:
    """Insert a settings assignment, cf. `shared.db.project_settings`."""
    ensure_connected()
    settings_doc = settings_by_unique_name(settings_id).get()
    doc = models.ProjectSettings(
        project=models.Project.objects.get(id=project_id),
        settings=settings_doc,
        scopes=scopes,
        excluded_scopes=excluded_scopes,
        raw_file_id_filter=raw_file_id_filter,
        raw_file_id_exclude_filter=raw_file_id_exclude_filter,
    )
    doc.save()
    return to_project_settings(doc, to_settings(settings_doc))


@translate_errors
def unassign(assignment_id: str) -> None:
    """Delete a settings assignment, cf. `shared.db.project_settings`."""
    ensure_connected()
    models.ProjectSettings.objects(id=assignment_id).delete()
