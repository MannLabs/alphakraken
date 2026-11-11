"""Queries on the assignments of settings to projects on MongoDB."""

import logging

from shared.db._mongo import models
from shared.db._mongo.connection import db_backend_function
from shared.db._mongo.mappers import reference_id, to_project_settings, to_settings
from shared.db._mongo.settings import settings_by_unique_name
from shared.domain.errors import NotFoundError
from shared.domain.projects import NewAssignment, ProjectSettings


@db_backend_function
def find(project_ids: list[str] | None) -> list[ProjectSettings]:
    """Return the settings assignments of the given projects, cf. `shared.db.project_settings`."""
    conditions = {} if project_ids is None else {"project__in": project_ids}
    docs = list(models.ProjectSettings.objects(**conditions).order_by("created_at_"))
    # one query for all settings, where dereferencing would run one per assignment
    settings_ids = {reference_id(doc, "settings") for doc in docs}
    settings_by_id = {
        settings_doc.pk: to_settings(settings_doc)
        for settings_doc in models.Settings.objects(id__in=settings_ids)
    }
    result = []
    for doc in docs:
        settings_id = reference_id(doc, "settings")
        if settings_id not in settings_by_id:
            # one dangling assignment must not hide all others
            logging.error(
                f"Skipping assignment {doc.pk} of project {reference_id(doc, 'project')}: "
                f"settings {settings_id} do not exist"
            )
            continue
        result.append(to_project_settings(doc, settings_by_id[settings_id]))
    return result


@db_backend_function
def get_project_id(assignment_id: str) -> str:
    """Return the id of the project of a settings assignment.

    The settings are not loaded, so this also works for an assignment whose settings no longer exist.
    """
    doc = models.ProjectSettings.objects(id=assignment_id).first()
    if doc is None:
        raise NotFoundError(f"No settings assignment {assignment_id}")
    return reference_id(doc, "project")


@db_backend_function
def assign(new: NewAssignment) -> ProjectSettings:
    """Insert a settings assignment, cf. `shared.db.project_settings`."""
    settings_doc = settings_by_unique_name(new.settings_id).get()
    doc = models.ProjectSettings(
        project=models.Project.objects.get(id=new.project_id),
        settings=settings_doc,
        **new.model_dump(exclude={"project_id", "settings_id"}),
    )
    doc.save()
    return to_project_settings(doc, to_settings(settings_doc))


@db_backend_function
def unassign(assignment_id: str) -> None:
    """Delete a settings assignment, cf. `shared.db.project_settings`."""
    models.ProjectSettings.objects(id=assignment_id).delete()
