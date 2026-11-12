"""Settings queries on MongoDB."""

from typing import Any

from mongoengine import QuerySet

from shared.db._mongo import models
from shared.db._mongo.connection import ensure_connected
from shared.db._mongo.errors import translate_errors
from shared.db._mongo.mappers import to_settings
from shared.domain.errors import NotFoundError
from shared.domain.settings import Settings, SettingsStatus, split_unique_settings_name


def _query(*, name: str | None, include_deleted: bool) -> QuerySet:
    conditions = {}
    if not include_deleted:
        conditions["status__ne"] = SettingsStatus.DELETED
    if name is not None:
        conditions["name"] = name
    return models.Settings.objects(**conditions)


@translate_errors
def find(
    *, name: str | None, include_deleted: bool, limit: int | None, offset: int
) -> list[Settings]:
    """Return the settings, cf. `shared.db.settings`."""
    ensure_connected()
    query = _query(name=name, include_deleted=include_deleted).order_by("-created_at_")
    if offset:
        query = query.skip(offset)
    if limit is not None:
        query = query.limit(limit)
    return [to_settings(doc) for doc in query]


@translate_errors
def count(*, name: str | None, include_deleted: bool) -> int:
    """Return the number of settings, cf. `shared.db.settings`."""
    ensure_connected()
    return _query(name=name, include_deleted=include_deleted).count()


@translate_errors
def get(unique_name: str) -> Settings:
    """Return the settings with the given unique name, cf. `shared.db.settings`."""
    ensure_connected()
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


@translate_errors
def create(*, owner_email: str | None, **fields: Any) -> Settings:
    """Insert new settings, cf. `shared.db.settings`."""
    ensure_connected()
    owner = models.User.objects.get(email=owner_email) if owner_email else None
    settings = models.Settings(owner=owner, **fields)
    settings.save(force_insert=True)
    return to_settings(settings)


@translate_errors
def set_status(unique_name: str, status: str) -> None:
    """Set the status of the settings with the given unique name."""
    ensure_connected()
    settings_by_unique_name(unique_name).update_one(status=status)
