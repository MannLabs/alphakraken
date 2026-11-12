"""Tests for the projects, settings and project settings APIs and their Mongo backends."""

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from bson import ObjectId

from shared.db import models, project_settings, projects, settings
from shared.db._mongo import settings as settings_backend
from shared.domain.errors import NotFoundError, ValidationError
from shared.domain.projects import ProjectStatus
from shared.domain.settings import SettingsStatus
from shared.tests import factories

NAIVE_TIME = datetime(2024, 1, 2, 3, 4, 5)  # noqa: DTZ001 as stored by pymongo
AWARE_TIME = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
SETTINGS_OBJECT_ID = ObjectId("65a0000000000000000000aa")
ASSIGNMENT_OBJECT_ID = ObjectId("65a0000000000000000000bb")


def _project_document(project_id: str = "P1") -> models.Project:
    return models.Project(
        id=project_id,
        name="Project 1",
        description="desc",
        owner=models.User(email="masc@example.org", initials="MaSc"),
        created_at_=NAIVE_TIME,
    )


def _settings_document() -> models.Settings:
    return models.Settings(
        id=SETTINGS_OBJECT_ID,
        number=1,
        name="fallback",
        version=7,
        software_type="alphadia",
        software="alphadia-1.0",
        runner_name="slurm",
        metrics_type="alphadia",
        created_at_=NAIVE_TIME,
    )


@pytest.fixture
def mock_models() -> Iterator[MagicMock]:
    """Patch the models of the three backends with one mock; chained query calls return the query."""
    mock = MagicMock()
    for model in ["Project", "Settings", "ProjectSettings"]:
        query = getattr(mock, model).objects.return_value
        for method in ["order_by", "skip", "limit"]:
            getattr(query, method).return_value = query
    with (
        patch("shared.db._mongo.projects.ensure_connected"),
        patch("shared.db._mongo.settings.ensure_connected"),
        patch("shared.db._mongo.project_settings.ensure_connected"),
        patch("shared.db._mongo.projects.models", mock),
        patch("shared.db._mongo.settings.models", mock),
        patch("shared.db._mongo.project_settings.models", mock),
    ):
        yield mock


# projects


def test_find_projects(mock_models: MagicMock) -> None:
    """Non-deleted projects come newest first, with the owner as email and a UTC-aware datetime."""
    query = mock_models.Project.objects.return_value
    query.__iter__.return_value = [_project_document()]

    result = projects.find(ids=["P1"], limit=2, offset=4)

    assert result == [
        factories.project(owner_email="masc@example.org", created_at_=AWARE_TIME)
    ]
    mock_models.Project.objects.assert_called_once_with(
        status__ne=ProjectStatus.DELETED, id__in=["P1"]
    )
    query.order_by.assert_called_once_with("-created_at_")
    query.skip.assert_called_once_with(4)
    query.limit.assert_called_once_with(2)


def test_find_projects_including_deleted_without_paging(mock_models: MagicMock) -> None:
    """Deleted projects are included on request; without limit and offset the query is not cut."""
    query = mock_models.Project.objects.return_value
    query.__iter__.return_value = []

    assert projects.find(include_deleted=True) == []
    mock_models.Project.objects.assert_called_once_with()
    query.skip.assert_not_called()
    query.limit.assert_not_called()


def test_count_projects(mock_models: MagicMock) -> None:
    """The count applies the same conditions as find."""
    mock_models.Project.objects.return_value.count.return_value = 7

    assert projects.count(ids=["P1"]) == 7
    mock_models.Project.objects.assert_called_once_with(
        status__ne=ProjectStatus.DELETED, id__in=["P1"]
    )


# settings


def test_find_settings(mock_models: MagicMock) -> None:
    """Non-deleted settings come newest first, with the unique name as id."""
    query = mock_models.Settings.objects.return_value
    query.__iter__.return_value = [_settings_document()]

    result = settings.find(name="fallback", limit=10, offset=20)

    assert result == [factories.settings(created_at_=AWARE_TIME)]
    assert result[0].unique_name == "fallback_v7"
    mock_models.Settings.objects.assert_called_once_with(
        status__ne=SettingsStatus.DELETED, name="fallback"
    )
    query.order_by.assert_called_once_with("-created_at_")
    query.skip.assert_called_once_with(20)
    query.limit.assert_called_once_with(10)


def test_count_settings(mock_models: MagicMock) -> None:
    """The count applies the same conditions as find."""
    mock_models.Settings.objects.return_value.count.return_value = 3

    assert settings.count(name="fallback", include_deleted=True) == 3
    mock_models.Settings.objects.assert_called_once_with(name="fallback")


def test_get_settings(mock_models: MagicMock) -> None:
    """Settings are looked up by the name and version in their unique name."""
    mock_models.Settings.objects.return_value.first.return_value = _settings_document()

    assert settings.get("fallback_v7") == factories.settings(created_at_=AWARE_TIME)
    mock_models.Settings.objects.assert_called_once_with(name="fallback", version=7)


def test_get_settings_raises_if_missing(mock_models: MagicMock) -> None:
    """Unknown settings are an error."""
    mock_models.Settings.objects.return_value.first.return_value = None

    with pytest.raises(NotFoundError, match="fallback_v99"):
        settings.get("fallback_v99")


def test_get_settings_raises_for_malformed_name(mock_models: MagicMock) -> None:
    """A name without version suffix cannot exist, so it is not queried."""
    with pytest.raises(NotFoundError):
        settings.get("fallback")
    mock_models.Settings.objects.assert_not_called()


# project settings


def test_find_project_settings_loads_settings_in_one_query(
    mock_models: MagicMock,
) -> None:
    """Assignments come oldest first with their settings embedded, loaded by one query."""
    settings_document = _settings_document()
    assignment = models.ProjectSettings(
        id=ASSIGNMENT_OBJECT_ID,
        project=_project_document("P1"),
        settings=settings_document,
        scopes=["test1"],
        raw_file_id_filter=["hela"],
        created_at_=NAIVE_TIME,
    )
    assignments_query = mock_models.ProjectSettings.objects.return_value
    assignments_query.__iter__.return_value = [assignment]
    mock_models.Settings.objects.return_value = [settings_document]

    result = project_settings.find(["P1", "P2"])

    assert result == [
        factories.project_settings(
            id=str(ASSIGNMENT_OBJECT_ID),
            project_id="P1",
            settings=factories.settings(created_at_=AWARE_TIME),
            scopes=["test1"],
            raw_file_id_filter=["hela"],
            created_at_=AWARE_TIME,
        )
    ]
    mock_models.ProjectSettings.objects.assert_called_once_with(
        project__in=["P1", "P2"]
    )
    assignments_query.order_by.assert_called_once_with("created_at_")
    mock_models.Settings.objects.assert_called_once_with(id__in={SETTINGS_OBJECT_ID})


def test_find_all_project_settings(mock_models: MagicMock) -> None:
    """Without project ids, the assignments of all projects are returned."""
    mock_models.ProjectSettings.objects.return_value.__iter__.return_value = []
    mock_models.Settings.objects.return_value = []

    assert project_settings.find() == []
    mock_models.ProjectSettings.objects.assert_called_once_with()


# settings writes: the rules are in the API module, the backend only stores

CREATE_FIELDS: dict[str, Any] = {
    "name": "fallback",
    "software_type": "alphadia",
    "software": "alphadia-1.0",
    "runner_name": "slurm",
    "metrics_type": "alphadia",
}


@patch("shared.db._mongo.settings.create")
@patch("shared.db.settings.count", return_value=41)
@patch("shared.db.settings.find")
def test_create_settings_as_next_version(
    mock_find: MagicMock, mock_count: MagicMock, mock_backend_create: MagicMock
) -> None:
    """New settings get the version after the highest one of their name, deleted ones included."""
    mock_find.return_value = [
        factories.settings(version=7),
        factories.settings(version=3, status=SettingsStatus.DELETED),
    ]

    result = settings.create(**CREATE_FIELDS, owner_email="a@b.de")

    mock_find.assert_called_once_with(name="fallback", include_deleted=True)
    mock_count.assert_called_once_with(include_deleted=True)
    stored = mock_backend_create.call_args.kwargs
    assert (stored["version"], stored["number"]) == (8, 42)
    assert stored["owner_email"] == "a@b.de"
    assert result == mock_backend_create.return_value


@patch("shared.db._mongo.settings.create")
@patch("shared.db.settings.count", return_value=0)
@patch("shared.db.settings.find", return_value=[])
def test_create_settings_first_version(
    mock_find: MagicMock,  # noqa: ARG001
    mock_count: MagicMock,  # noqa: ARG001
    mock_backend_create: MagicMock,
) -> None:
    """Settings with a new name start at version 1."""
    settings.create(**CREATE_FIELDS)

    stored = mock_backend_create.call_args.kwargs
    assert (stored["version"], stored["number"]) == (1, 1)


def test_create_settings_requires_runner() -> None:
    """Settings cannot be created without naming the runner."""
    fields: dict[str, Any] = {
        k: v for k, v in CREATE_FIELDS.items() if k != "runner_name"
    }

    with pytest.raises(TypeError, match="runner_name"):
        settings.create(**fields)


def test_backend_create_settings_resolves_owner(mock_models: MagicMock) -> None:
    """The backend inserts the settings with the owner looked up by email."""
    mock_models.Settings.return_value = _settings_document()

    with patch.object(models.Settings, "save") as mock_save:
        result = settings_backend.create(
            owner_email="a@b.de", number=1, **CREATE_FIELDS
        )

    mock_models.User.objects.get.assert_called_once_with(email="a@b.de")
    mock_models.Settings.assert_called_once_with(
        owner=mock_models.User.objects.get.return_value, number=1, **CREATE_FIELDS
    )
    mock_save.assert_called_once_with(force_insert=True)
    assert result == factories.settings(created_at_=AWARE_TIME)


@patch("shared.db._mongo.settings.set_status")
def test_archive_settings(mock_set_status: MagicMock) -> None:
    """Archiving sets the status to inactive."""
    settings.archive("fallback_v7")

    mock_set_status.assert_called_once_with("fallback_v7", SettingsStatus.INACTIVE)


def test_backend_set_status_by_unique_name(mock_models: MagicMock) -> None:
    """The backend finds the settings by the name and version in the unique name."""
    settings_backend.set_status("fallback_v7", SettingsStatus.INACTIVE)

    mock_models.Settings.objects.assert_called_once_with(name="fallback", version=7)
    mock_models.Settings.objects.return_value.update_one.assert_called_once_with(
        status=SettingsStatus.INACTIVE
    )


@patch("shared.db._mongo.settings.set_status")
@patch("shared.db.project_settings.find")
@patch("shared.db.settings.get")
def test_delete_settings(
    mock_get: MagicMock, mock_find_assignments: MagicMock, mock_set_status: MagicMock
) -> None:
    """Active settings that are not assigned are soft-deleted."""
    mock_get.return_value = factories.settings()
    # assigned are other settings only
    mock_find_assignments.return_value = [
        factories.project_settings(settings=factories.settings(id="other_v1"))
    ]

    settings.delete("fallback_v7")

    mock_set_status.assert_called_once_with("fallback_v7", SettingsStatus.DELETED)


@patch("shared.db._mongo.settings.set_status")
@patch("shared.db.project_settings.find", return_value=[])
@patch("shared.db.settings.get")
def test_delete_archived_settings_raises(
    mock_get: MagicMock,
    mock_find_assignments: MagicMock,  # noqa: ARG001
    mock_set_status: MagicMock,
) -> None:
    """Archived settings cannot be deleted."""
    mock_get.return_value = factories.settings(status=SettingsStatus.INACTIVE)

    with pytest.raises(ValidationError, match="Cannot delete archived settings"):
        settings.delete("fallback_v7")

    mock_set_status.assert_not_called()


@patch("shared.db._mongo.settings.set_status")
@patch("shared.db.project_settings.find")
@patch("shared.db.settings.get")
def test_delete_assigned_settings_raises(
    mock_get: MagicMock, mock_find_assignments: MagicMock, mock_set_status: MagicMock
) -> None:
    """Settings assigned to any project, also an archived or deleted one, cannot be deleted."""
    mock_get.return_value = factories.settings()
    mock_find_assignments.return_value = [
        factories.project_settings(project_id="P2"),
        factories.project_settings(project_id="P1"),
    ]

    with pytest.raises(ValidationError, match="still assigned to projects P1, P2"):
        settings.delete("fallback_v7")

    mock_find_assignments.assert_called_once_with()
    mock_set_status.assert_not_called()
