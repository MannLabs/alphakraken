"""Settings queries on MongoDB."""

from mongoengine import QuerySet

from shared.db._mongo import models
from shared.db._mongo.connection import db_backend_function
from shared.db._mongo.mappers import reference_id, to_settings
from shared.domain.errors import NotFoundError
from shared.domain.settings import (
    NewSettings,
    Settings,
    SettingsStatus,
    split_unique_settings_name,
)


def _query(
    *, name: str | None, statuses: list[str] | None, include_deleted: bool
) -> QuerySet:
    conditions = {}
    if not include_deleted:
        conditions["status__ne"] = SettingsStatus.DELETED
    if statuses is not None:
        conditions["status__in"] = statuses
    if name is not None:
        conditions["name"] = name
    return models.Settings.objects(**conditions)


@db_backend_function
def find(
    *,
    name: str | None,
    statuses: list[str] | None,
    include_deleted: bool,
    limit: int | None,
    offset: int,
) -> list[Settings]:
    """Return the settings, cf. `shared.db.settings`."""
    query = _query(
        name=name, statuses=statuses, include_deleted=include_deleted
    ).order_by("-created_at_")
    if offset:
        query = query.skip(offset)
    if limit is not None:
        query = query.limit(limit)
    return [to_settings(doc) for doc in query]


@db_backend_function
def count(
    *, name: str | None, statuses: list[str] | None, include_deleted: bool
) -> int:
    """Return the number of settings, cf. `shared.db.settings`."""
    return _query(name=name, statuses=statuses, include_deleted=include_deleted).count()


@db_backend_function
def get(unique_name: str) -> Settings:
    """Return the settings with the given unique name, cf. `shared.db.settings`."""
    doc = settings_by_unique_name(unique_name).first()
    if doc is None:
        raise NotFoundError(f"No settings {unique_name}")
    return to_settings(doc)


def settings_by_unique_name(unique_name: str) -> QuerySet:
    """Return the query for the settings with the given unique name, e.g. `fallback_v7`."""
    try:
        name, version = split_unique_settings_name(unique_name)
    except ValueError as e:
        raise NotFoundError(str(e)) from e
    return models.Settings.objects(name=name, version=version)


@db_backend_function
def assigned_project_ids(unique_name: str) -> list[str]:
    """Return the ids of the projects the settings are assigned to, sorted."""
    settings = settings_by_unique_name(unique_name).first()
    if settings is None:
        raise NotFoundError(f"No settings {unique_name}")
    # only these assignments are read, so that a broken one of other settings does not matter
    assignments = models.ProjectSettings.objects(settings=settings)
    return sorted({reference_id(doc, "project") for doc in assignments})


@db_backend_function
def create(new: NewSettings, *, number: int, version: int) -> Settings:
    """Insert new settings, cf. `shared.db.settings`."""
    owner = models.User.objects.get(email=new.owner_email) if new.owner_email else None
    settings = models.Settings(
        owner=owner,
        number=number,
        version=version,
        **new.model_dump(exclude={"owner_email"}),
    )
    settings.save(force_insert=True)
    return to_settings(settings)


@db_backend_function
def set_status(unique_name: str, status: str) -> None:
    """Set the status of the settings with the given unique name."""
    if not settings_by_unique_name(unique_name).update_one(status=status):
        raise NotFoundError(f"No settings {unique_name}")
