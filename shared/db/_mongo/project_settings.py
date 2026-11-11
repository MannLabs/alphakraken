"""Queries on the assignments of settings to projects on MongoDB."""

from shared.db import models
from shared.db._mongo.connection import ensure_connected
from shared.db._mongo.errors import translate_errors
from shared.db._mongo.mappers import to_project_settings, to_settings
from shared.domain.projects import ProjectSettings


@translate_errors
def find(project_ids: list[str]) -> list[ProjectSettings]:
    """Return the settings assignments of the given projects, cf. `shared.db.project_settings`."""
    ensure_connected()
    docs = list(
        models.ProjectSettings.objects(project__in=project_ids).order_by("created_at_")
    )
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
