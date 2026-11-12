"""Tests for the projects, settings and project settings APIs and their Mongo backends."""

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest
from bson import ObjectId

from shared.db import project_settings, projects, settings
from shared.db._mongo import models
from shared.db._mongo import project_settings as project_settings_backend
from shared.db._mongo import projects as projects_backend
from shared.db._mongo import settings as settings_backend
from shared.domain.errors import NotFoundError, ValidationError
from shared.domain.projects import NewProject, ProjectStatus
from shared.domain.settings import NewSettings, SettingsStatus
from shared.domain.users import UserStatus
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
        patch("shared.db._mongo.connection.ensure_connected"),
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


def test_find_and_count_projects_by_status(mock_models: MagicMock) -> None:
    """The statuses narrow the projects in the query, for `find` and `count` alike."""
    query = mock_models.Project.objects.return_value
    query.__iter__.return_value = []
    query.count.return_value = 0

    projects.find(statuses=[ProjectStatus.ACTIVE])
    projects.count(statuses=[ProjectStatus.ACTIVE])

    assert (
        mock_models.Project.objects.call_args_list
        == [call(status__ne=ProjectStatus.DELETED, status__in=[ProjectStatus.ACTIVE])]
        * 2
    )


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


def test_find_and_count_settings_by_status(mock_models: MagicMock) -> None:
    """The statuses narrow the settings in the query, for `find` and `count` alike."""
    query = mock_models.Settings.objects.return_value
    query.__iter__.return_value = []
    query.count.return_value = 0

    settings.find(statuses=[SettingsStatus.ACTIVE])
    settings.count(statuses=[SettingsStatus.ACTIVE])

    assert (
        mock_models.Settings.objects.call_args_list
        == [call(status__ne=SettingsStatus.DELETED, status__in=[SettingsStatus.ACTIVE])]
        * 2
    )


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


@pytest.fixture
def updatable(assignable: dict[str, MagicMock]) -> Iterator[dict[str, MagicMock]]:
    """Assignment `assignment1` of `fallback_v7` to `P1`, with `fallback_v8` as latest active version."""
    assignable["find_assignments"].return_value = [
        factories.project_settings(
            scopes=["thermo"],
            excluded_scopes=["test1"],
            raw_file_id_filter=["a"],
            raw_file_id_exclude_filter=["blank"],
        )
    ]
    with patch(
        "shared.db.settings.latest_active",
        return_value=factories.settings(id="fallback_v8", version=8),
    ) as latest_active:
        yield {**assignable, "latest_active": latest_active}


def test_update_assignment_to_latest(updatable: dict[str, MagicMock]) -> None:
    """The latest version is assigned with the same scopes and filters, then the old assignment is removed."""
    updatable["backend_assign"].side_effect = lambda *_, **__: updatable[
        "backend_unassign"
    ].assert_not_called()

    project_settings.update_to_latest("assignment1")

    updatable["latest_active"].assert_called_once_with("fallback")
    updatable["backend_assign"].assert_called_once_with(
        "P1",
        "fallback_v8",
        scopes=["thermo"],
        excluded_scopes=["test1"],
        raw_file_id_filter=["a"],
        raw_file_id_exclude_filter=["blank"],
    )
    updatable["backend_unassign"].assert_called_once_with("assignment1")


@pytest.mark.parametrize("latest", [None, factories.settings()])
def test_update_assignment_without_newer_version_raises(
    updatable: dict[str, MagicMock], latest: Any
) -> None:
    """Without a newer active version, the assignment is left alone."""
    updatable["latest_active"].return_value = latest

    with pytest.raises(ValidationError, match="No newer active version"):
        project_settings.update_to_latest("assignment1")

    updatable["backend_assign"].assert_not_called()
    updatable["backend_unassign"].assert_not_called()


def test_update_assignment_keeps_old_one_if_assigning_fails(
    updatable: dict[str, MagicMock],
) -> None:
    """A failed assignment of the latest version does not cost the current one."""
    updatable["find_projects"].return_value = [
        factories.project(status=ProjectStatus.INACTIVE)
    ]

    with pytest.raises(ValidationError, match="Cannot assign settings"):
        project_settings.update_to_latest("assignment1")

    updatable["backend_unassign"].assert_not_called()


# settings writes: the rules are in the API module, the backend only stores

CREATE_FIELDS: dict[str, Any] = {
    "fasta_file_name": "human.fasta",
    "software_type": "alphadia",
    "software": "alphadia-1.0",
    "runner_name": "slurm",
    "metrics_type": "alphadia",
}


@pytest.fixture
def creatable() -> Iterator[dict[str, MagicMock]]:
    """No settings exist, the runner check passes; the backend writes are mocked."""
    with (
        patch("shared.db.settings.find", return_value=[]) as find,
        patch("shared.db.settings.count", return_value=0) as count,
        patch("shared.db.settings.get") as get,
        patch(
            "shared.db.settings.check_runner_supports_software_type", return_value=[]
        ) as check_runner,
        patch("shared.db.users.require_active") as require_active,
        patch("shared.db._mongo.settings.create") as backend_create,
        patch("shared.db._mongo.settings.set_status") as backend_set_status,
    ):
        get.return_value = factories.settings()
        yield {
            "find": find,
            "count": count,
            "get": get,
            "check_runner": check_runner,
            "require_active": require_active,
            "backend_create": backend_create,
            "backend_set_status": backend_set_status,
        }


def test_create_settings_as_next_version(creatable: dict[str, MagicMock]) -> None:
    """New settings get the version after the highest one of their name, deleted ones included."""
    creatable["find"].return_value = [
        factories.settings(version=7),
        factories.settings(version=3, status=SettingsStatus.DELETED),
    ]
    creatable["count"].return_value = 41

    result = settings.create(**CREATE_FIELDS, name=" fallback ", owner_email="a@b.de")

    creatable["find"].assert_called_once_with(name="fallback", include_deleted=True)
    creatable["count"].assert_called_once_with(include_deleted=True)
    creatable["check_runner"].assert_called_once_with("slurm", "alphadia")
    creatable["require_active"].assert_called_once_with("a@b.de")
    creatable["backend_create"].assert_called_once_with(
        NewSettings(**CREATE_FIELDS, name="fallback", owner_email="a@b.de"),
        number=42,
        version=8,
    )
    assert result == creatable["backend_create"].return_value


def test_create_settings_first_version(creatable: dict[str, MagicMock]) -> None:
    """Settings with a new name start at version 1, an owner is not needed."""
    settings.create(**CREATE_FIELDS, name="fallback")

    stored = creatable["backend_create"].call_args.kwargs
    assert (stored["version"], stored["number"]) == (1, 1)
    creatable["require_active"].assert_not_called()


def test_create_settings_with_invalid_field_raises(
    creatable: dict[str, MagicMock],
) -> None:
    """Invalid fields are a validation error before the database is asked."""
    with pytest.raises(ValidationError) as e:
        settings.create(**CREATE_FIELDS, name="fall back", slurm_mem="lots")

    assert e.value.errors == [
        "name: String should match pattern '^[a-zA-Z0-9_]+$'",
        "slurm_mem: Memory must be a number followed by a unit (K, M, G, or T), e.g. '62G'.",
    ]
    creatable["find"].assert_not_called()
    creatable["backend_create"].assert_not_called()


def test_create_settings_on_unsuitable_runner_raises(
    creatable: dict[str, MagicMock],
) -> None:
    """A runner that is unknown or cannot run the software type is a validation error."""
    creatable["check_runner"].return_value = ["Unknown runner 'slurm'"]

    with pytest.raises(ValidationError, match="Unknown runner 'slurm'"):
        settings.create(**CREATE_FIELDS, name="fallback")

    creatable["backend_create"].assert_not_called()


def test_create_settings_archiving_previous(creatable: dict[str, MagicMock]) -> None:
    """The latest non-deleted version of the name is archived after the new one is created."""
    creatable["find"].return_value = [
        factories.settings(id="fallback_v8", version=8, status=SettingsStatus.DELETED),
        factories.settings(id="fallback_v7", version=7),
        factories.settings(id="fallback_v6", version=6),
    ]
    creatable["backend_create"].side_effect = lambda *_, **__: creatable[
        "backend_set_status"
    ].assert_not_called()

    settings.create(**CREATE_FIELDS, name="fallback", archive_previous=True)

    creatable["backend_set_status"].assert_called_once_with(
        "fallback_v7", SettingsStatus.INACTIVE
    )


@pytest.mark.parametrize(
    ("existing", "archive_previous"),
    [([], True), ([factories.settings()], False)],
)
def test_create_settings_without_archiving(
    creatable: dict[str, MagicMock], existing: list[Any], *, archive_previous: bool
) -> None:
    """Nothing is archived without a previous version or without being asked to."""
    creatable["find"].return_value = existing

    settings.create(**CREATE_FIELDS, name="fallback", archive_previous=archive_previous)

    creatable["backend_set_status"].assert_not_called()


@pytest.mark.parametrize("owner_email", ["a@b.de", None])
def test_backend_create_settings_resolves_owner(
    mock_models: MagicMock, owner_email: str | None
) -> None:
    """The backend inserts the settings with number, version and the owner looked up by email."""
    mock_models.Settings.return_value = _settings_document()
    new = NewSettings(**CREATE_FIELDS, name="fallback", owner_email=owner_email)

    with patch.object(models.Settings, "save") as mock_save:
        result = settings_backend.create(new, number=1, version=7)

    stored = mock_models.Settings.call_args.kwargs
    if owner_email:
        mock_models.User.objects.get.assert_called_once_with(email="a@b.de")
        assert stored["owner"] == mock_models.User.objects.get.return_value
    else:
        mock_models.User.objects.get.assert_not_called()
        assert stored["owner"] is None
    assert (stored["number"], stored["version"], stored["name"]) == (1, 7, "fallback")
    assert "owner_email" not in stored
    assert stored.keys() - {
        "owner",
        "number",
        "version",
    } == NewSettings.model_fields.keys() - {"owner_email"}
    mock_save.assert_called_once_with(force_insert=True)
    assert result == factories.settings(created_at_=AWARE_TIME)


@pytest.mark.parametrize(
    ("status", "archived"),
    [
        (SettingsStatus.ACTIVE, True),
        (SettingsStatus.INACTIVE, True),
        (SettingsStatus.DELETED, False),
    ],
)
@patch("shared.db._mongo.settings.set_status")
@patch("shared.db.settings.get")
def test_archive_settings_unless_deleted(
    mock_get: MagicMock, mock_set_status: MagicMock, status: str, *, archived: bool
) -> None:
    """Archiving sets the status to inactive, deleted settings stay deleted."""
    mock_get.return_value = factories.settings(status=status)

    if archived:
        settings.archive("fallback_v7")
        mock_set_status.assert_called_once_with("fallback_v7", SettingsStatus.INACTIVE)
    else:
        with pytest.raises(ValidationError, match="Cannot archive deleted settings"):
            settings.archive("fallback_v7")
        mock_set_status.assert_not_called()


def test_backend_set_status_by_unique_name(mock_models: MagicMock) -> None:
    """The backend finds the settings by the name and version in the unique name."""
    settings_backend.set_status("fallback_v7", SettingsStatus.INACTIVE)

    mock_models.Settings.objects.assert_called_once_with(name="fallback", version=7)
    mock_models.Settings.objects.return_value.update_one.assert_called_once_with(
        status=SettingsStatus.INACTIVE
    )


def test_backend_set_status_of_unknown_settings_raises(mock_models: MagicMock) -> None:
    """Setting the status of settings that do not exist is an error."""
    mock_models.Settings.objects.return_value.update_one.return_value = 0

    with pytest.raises(NotFoundError, match="fallback_v7"):
        settings_backend.set_status("fallback_v7", SettingsStatus.INACTIVE)


def test_backend_set_status_of_project(mock_models: MagicMock) -> None:
    """The backend sets the status of the project with the given id."""
    projects_backend.set_status("P1", ProjectStatus.INACTIVE)

    mock_models.Project.objects.assert_called_once_with(id="P1")
    mock_models.Project.objects.return_value.update_one.assert_called_once_with(
        status=ProjectStatus.INACTIVE
    )


def test_backend_set_status_of_unknown_project_raises(mock_models: MagicMock) -> None:
    """Setting the status of a project that does not exist is an error."""
    mock_models.Project.objects.return_value.update_one.return_value = 0

    with pytest.raises(NotFoundError, match="P1"):
        projects_backend.set_status("P1", ProjectStatus.INACTIVE)


@patch("shared.db._mongo.settings.set_status")
@patch("shared.db._mongo.settings.assigned_project_ids", return_value=[])
@patch("shared.db.settings.get")
def test_delete_settings(
    mock_get: MagicMock, mock_assigned: MagicMock, mock_set_status: MagicMock
) -> None:
    """Active settings that are not assigned are soft-deleted."""
    mock_get.return_value = factories.settings()

    settings.delete("fallback_v7")

    mock_assigned.assert_called_once_with("fallback_v7")
    mock_set_status.assert_called_once_with("fallback_v7", SettingsStatus.DELETED)


@patch("shared.db._mongo.settings.set_status")
@patch("shared.db._mongo.settings.assigned_project_ids", return_value=[])
@patch("shared.db.settings.get")
def test_delete_archived_settings_raises(
    mock_get: MagicMock,
    mock_assigned: MagicMock,  # noqa: ARG001
    mock_set_status: MagicMock,
) -> None:
    """Archived settings cannot be deleted."""
    mock_get.return_value = factories.settings(status=SettingsStatus.INACTIVE)

    with pytest.raises(ValidationError, match="Cannot delete archived settings"):
        settings.delete("fallback_v7")

    mock_set_status.assert_not_called()


@patch("shared.db._mongo.settings.set_status")
@patch("shared.db._mongo.settings.assigned_project_ids", return_value=["P1", "P2"])
@patch("shared.db.settings.get")
def test_delete_assigned_settings_raises(
    mock_get: MagicMock,
    mock_assigned: MagicMock,
    mock_set_status: MagicMock,
) -> None:
    """Settings assigned to any project, also an archived or deleted one, cannot be deleted."""
    mock_get.return_value = factories.settings()

    with pytest.raises(ValidationError, match="still assigned to projects P1, P2"):
        settings.delete("fallback_v7")

    mock_assigned.assert_called_once_with("fallback_v7")
    mock_set_status.assert_not_called()


# project writes


@patch("shared.db.users.require_active")
@patch("shared.db._mongo.projects.add")
@patch("shared.db.projects.find", return_value=[])
def test_add_project(
    mock_find: MagicMock, mock_backend_add: MagicMock, mock_require_active: MagicMock
) -> None:
    """A project with an unused id and an active owner is stored, with the fields cleaned."""
    projects.add(project_id=" P123 ", name="n", description=" ", owner_email="a@b.de")

    mock_find.assert_called_once_with(ids=["P123"], include_deleted=True)
    mock_require_active.assert_called_once_with("a@b.de")
    mock_backend_add.assert_called_once_with(
        NewProject(project_id="P123", name="n", owner_email="a@b.de")
    )


@patch("shared.db.users.require_active")
@patch("shared.db._mongo.projects.add")
@patch("shared.db.projects.find", return_value=[])
def test_add_project_without_owner(
    mock_find: MagicMock,  # noqa: ARG001
    mock_backend_add: MagicMock,
    mock_require_active: MagicMock,
) -> None:
    """A project needs no owner."""
    projects.add(project_id="P123", name="n", owner_email="")

    mock_require_active.assert_not_called()
    mock_backend_add.assert_called_once()


@patch("shared.db._mongo.projects.add")
@patch("shared.db.projects.find")
def test_add_project_with_invalid_field_raises(
    mock_find: MagicMock, mock_backend_add: MagicMock
) -> None:
    """Invalid fields are a validation error before the database is asked."""
    with pytest.raises(ValidationError, match="project_id: Invalid project id 'p1'"):
        projects.add(project_id="p1", name="n")

    mock_find.assert_not_called()
    mock_backend_add.assert_not_called()


@patch("shared.db._mongo.projects.add")
@patch("shared.db.projects.find")
def test_add_project_with_id_of_deleted_project_raises(
    mock_find: MagicMock, mock_backend_add: MagicMock
) -> None:
    """The id of a deleted project cannot be reused."""
    mock_find.return_value = [factories.project(status=ProjectStatus.DELETED)]

    with pytest.raises(ValidationError, match="was used by a deleted project"):
        projects.add(project_id="P123", name="n", description="d")

    mock_backend_add.assert_not_called()


@patch("shared.db._mongo.users.get")
@patch("shared.db._mongo.projects.add")
@patch("shared.db.projects.find", return_value=[])
def test_add_project_with_inactive_owner_raises(
    mock_find: MagicMock,  # noqa: ARG001
    mock_backend_add: MagicMock,
    mock_get_user: MagicMock,
) -> None:
    """Only an active user can own a new project."""
    mock_get_user.return_value = factories.user(status=UserStatus.INACTIVE)

    with pytest.raises(ValidationError, match="User 'a@b.de' is inactive"):
        projects.add(project_id="P123", name="n", owner_email="a@b.de")

    mock_backend_add.assert_not_called()


@pytest.mark.parametrize("owner_email", ["a@b.de", None])
def test_backend_add_project_resolves_owner(
    mock_models: MagicMock, owner_email: str | None
) -> None:
    """The backend inserts the project, never overwriting one, with the owner looked up by email."""
    projects_backend.add(
        NewProject(
            project_id="P123", name="n", description="d", owner_email=owner_email
        )
    )

    if owner_email:
        mock_models.User.objects.get.assert_called_once_with(email="a@b.de")
    else:
        mock_models.User.objects.get.assert_not_called()
    mock_models.Project.assert_called_once_with(
        id="P123",
        name="n",
        description="d",
        owner=mock_models.User.objects.get.return_value if owner_email else None,
    )
    mock_models.Project.return_value.save.assert_called_once_with(force_insert=True)


@pytest.mark.parametrize(
    ("status", "archived"),
    [
        (ProjectStatus.ACTIVE, True),
        (ProjectStatus.INACTIVE, True),
        (ProjectStatus.DELETED, False),
    ],
)
@patch("shared.db._mongo.projects.set_status")
@patch("shared.db.projects.find")
def test_archive_project_unless_deleted(
    mock_find: MagicMock, mock_set_status: MagicMock, status: str, *, archived: bool
) -> None:
    """Archiving sets the status to inactive, a deleted project stays deleted."""
    mock_find.return_value = [factories.project(status=status)]

    if archived:
        projects.archive("P1")
        mock_set_status.assert_called_once_with("P1", ProjectStatus.INACTIVE)
    else:
        with pytest.raises(ValidationError, match="Cannot archive deleted project"):
            projects.archive("P1")
        mock_set_status.assert_not_called()


@pytest.mark.parametrize(
    ("status", "deleted"),
    [(ProjectStatus.ACTIVE, True), (ProjectStatus.INACTIVE, False)],
)
@patch("shared.db._mongo.projects.set_status")
@patch("shared.db.projects.find")
def test_delete_project_unless_archived(
    mock_find: MagicMock, mock_set_status: MagicMock, status: str, *, deleted: bool
) -> None:
    """Active projects are soft-deleted, archived ones cannot be deleted."""
    mock_find.return_value = [factories.project(status=status)]

    if deleted:
        projects.delete("P1")
        mock_set_status.assert_called_once_with("P1", ProjectStatus.DELETED)
    else:
        with pytest.raises(ValidationError, match="Cannot delete archived project"):
            projects.delete("P1")
        mock_set_status.assert_not_called()


@patch("shared.db.projects.find", return_value=[])
def test_get_project_raises_if_missing(mock_find: MagicMock) -> None:  # noqa: ARG001
    """An unknown project is an error."""
    with pytest.raises(NotFoundError, match="P9"):
        projects.get("P9")


# latest active settings


@patch("shared.db.settings.find")
def test_latest_active_settings(mock_find: MagicMock) -> None:
    """The highest active version of the name is returned; archived versions do not count."""
    mock_find.return_value = [
        factories.settings(id="fallback_v7", version=7),
        factories.settings(id="fallback_v8", version=8),
    ]

    assert settings.latest_active("fallback") == factories.settings(
        id="fallback_v8", version=8
    )
    mock_find.assert_called_once_with(name="fallback", statuses=[SettingsStatus.ACTIVE])


@patch("shared.db.settings.find", return_value=[])
def test_latest_active_settings_without_any(mock_find: MagicMock) -> None:  # noqa: ARG001
    """Without active settings of that name there is none."""
    assert settings.latest_active("fallback") is None


# assignments


@pytest.fixture
def assignable() -> Iterator[dict[str, MagicMock]]:
    """An active project `P1` and active settings `fallback_v7` without assignments."""
    with (
        patch("shared.db.projects.find") as find_projects,
        patch("shared.db.settings.get") as get_settings,
        patch("shared.db.project_settings.find", return_value=[]) as find_assignments,
        patch("shared.db._mongo.project_settings.assign") as backend_assign,
        patch("shared.db._mongo.project_settings.unassign") as backend_unassign,
        patch(
            "shared.db._mongo.project_settings.get_project_id", return_value="P1"
        ) as backend_get_project_id,
    ):
        find_projects.return_value = [factories.project()]
        get_settings.return_value = factories.settings()
        yield {
            "find_projects": find_projects,
            "get_settings": get_settings,
            "find_assignments": find_assignments,
            "backend_assign": backend_assign,
            "backend_unassign": backend_unassign,
            "backend_get_project_id": backend_get_project_id,
        }


def test_assign_settings_with_defaults(assignable: dict[str, MagicMock]) -> None:
    """Without scopes and filters, the assignment applies to all instruments and files."""
    result = project_settings.assign("P1", "fallback_v7")

    assignable["backend_assign"].assert_called_once_with(
        "P1",
        "fallback_v7",
        scopes=["*"],
        excluded_scopes=[],
        raw_file_id_filter=[],
        raw_file_id_exclude_filter=[],
    )
    assert result == assignable["backend_assign"].return_value


def test_assign_settings_default_scope_absorbs_other_scopes(
    assignable: dict[str, MagicMock],
) -> None:
    """The default scope matches everything, so other scopes next to it are dropped."""
    project_settings.assign(
        "P1",
        "fallback_v7",
        scopes=["test1", "*"],
        excluded_scopes=["test2"],
        raw_file_id_filter=["hela"],
        raw_file_id_exclude_filter=["blank"],
    )

    assignable["backend_assign"].assert_called_once_with(
        "P1",
        "fallback_v7",
        scopes=["*"],
        excluded_scopes=["test2"],
        raw_file_id_filter=["hela"],
        raw_file_id_exclude_filter=["blank"],
    )


@pytest.mark.parametrize("status", [ProjectStatus.INACTIVE, ProjectStatus.DELETED])
def test_assign_settings_to_archived_or_deleted_project_raises(
    assignable: dict[str, MagicMock], status: str
) -> None:
    """Assignments of archived and deleted projects are frozen."""
    assignable["find_projects"].return_value = [factories.project(status=status)]

    with pytest.raises(ValidationError, match="archived or deleted project 'P1'"):
        project_settings.assign("P1", "fallback_v7")

    assignable["backend_assign"].assert_not_called()


@pytest.mark.parametrize("status", [SettingsStatus.INACTIVE, SettingsStatus.DELETED])
def test_assign_archived_or_deleted_settings_raises(
    assignable: dict[str, MagicMock], status: str
) -> None:
    """Archived and deleted settings cannot be assigned."""
    assignable["get_settings"].return_value = factories.settings(status=status)

    with pytest.raises(
        ValidationError, match="Cannot assign archived or deleted settings"
    ):
        project_settings.assign("P1", "fallback_v7")

    assignable["backend_assign"].assert_not_called()


def test_assign_same_settings_with_same_filters_raises(
    assignable: dict[str, MagicMock],
) -> None:
    """The same settings with the same filters, exclusion compared case-insensitively, are a duplicate."""
    assignable["find_assignments"].return_value = [
        factories.project_settings(
            raw_file_id_filter=["a", "b"], raw_file_id_exclude_filter=["Blank"]
        )
    ]

    with pytest.raises(ValidationError, match="already assigned"):
        project_settings.assign(
            "P1",
            "fallback_v7",
            raw_file_id_filter=["b", "a"],
            raw_file_id_exclude_filter=["blank"],
        )

    assignable["find_assignments"].assert_called_once_with(["P1"])
    assignable["backend_assign"].assert_not_called()


def test_assign_same_settings_with_different_filters_succeeds(
    assignable: dict[str, MagicMock],
) -> None:
    """The same settings may be assigned again for other files."""
    assignable["find_assignments"].return_value = [
        factories.project_settings(raw_file_id_filter=["a"])
    ]

    project_settings.assign("P1", "fallback_v7", raw_file_id_filter=["b"])

    assignable["backend_assign"].assert_called_once()


def test_unassign_settings(assignable: dict[str, MagicMock]) -> None:
    """An assignment of an active project is removed."""
    project_settings.unassign("assignment1")

    assignable["backend_get_project_id"].assert_called_once_with("assignment1")
    assignable["find_assignments"].assert_not_called()
    assignable["backend_unassign"].assert_called_once_with("assignment1")


def test_unassign_settings_from_archived_project_raises(
    assignable: dict[str, MagicMock],
) -> None:
    """Assignments of archived and deleted projects are frozen."""
    assignable["find_projects"].return_value = [
        factories.project(status=ProjectStatus.INACTIVE)
    ]

    with pytest.raises(ValidationError, match="Cannot unassign settings"):
        project_settings.unassign("assignment1")

    assignable["backend_unassign"].assert_not_called()


def test_unassign_unknown_assignment_raises(assignable: dict[str, MagicMock]) -> None:
    """An unknown assignment is an error."""
    assignable["backend_get_project_id"].side_effect = NotFoundError("No assignment9")

    with pytest.raises(NotFoundError, match="assignment9"):
        project_settings.unassign("assignment9")

    assignable["backend_unassign"].assert_not_called()


def test_backend_assign_and_unassign(mock_models: MagicMock) -> None:
    """The backend stores the assignment with references to the project and the settings, and deletes by id."""
    settings_document = _settings_document()
    mock_models.Settings.objects.return_value.get.return_value = settings_document
    mock_models.ProjectSettings.return_value = models.ProjectSettings(
        id=ASSIGNMENT_OBJECT_ID,
        project=_project_document("P1"),
        settings=settings_document,
        scopes=["*"],
        created_at_=NAIVE_TIME,
    )

    with patch.object(models.ProjectSettings, "save") as mock_save:
        result = project_settings_backend.assign(
            "P1",
            "fallback_v7",
            scopes=["*"],
            excluded_scopes=[],
            raw_file_id_filter=[],
            raw_file_id_exclude_filter=[],
        )
    project_settings_backend.unassign("some_id")

    mock_models.Settings.objects.assert_called_once_with(name="fallback", version=7)
    mock_models.Project.objects.get.assert_called_once_with(id="P1")
    mock_save.assert_called_once()
    assert result == factories.project_settings(
        id=str(ASSIGNMENT_OBJECT_ID),
        settings=factories.settings(created_at_=AWARE_TIME),
        created_at_=AWARE_TIME,
    )
    mock_models.ProjectSettings.objects.assert_called_once_with(id="some_id")
    mock_models.ProjectSettings.objects.return_value.delete.assert_called_once()


def test_backend_get_project_id_of_assignment_without_loading_settings(
    mock_models: MagicMock,
) -> None:
    """The project id is read from the assignment alone, so its settings may be missing."""
    mock_models.ProjectSettings.objects.return_value.first.return_value = (
        models.ProjectSettings(
            id=ASSIGNMENT_OBJECT_ID,
            project=_project_document("P1"),
            settings=_settings_document(),
        )
    )

    result = project_settings_backend.get_project_id(str(ASSIGNMENT_OBJECT_ID))

    assert result == "P1"
    mock_models.ProjectSettings.objects.assert_called_once_with(
        id=str(ASSIGNMENT_OBJECT_ID)
    )
    mock_models.Settings.objects.assert_not_called()


def test_backend_get_project_id_of_unknown_assignment_raises(
    mock_models: MagicMock,
) -> None:
    """An unknown assignment id is an error."""
    mock_models.ProjectSettings.objects.return_value.first.return_value = None

    with pytest.raises(NotFoundError, match="some_id"):
        project_settings_backend.get_project_id("some_id")


def test_backend_assigned_project_ids(mock_models: MagicMock) -> None:
    """Only the assignments of the given settings are read, their project ids come sorted and unique."""
    settings_document = _settings_document()
    mock_models.Settings.objects.return_value.first.return_value = settings_document
    mock_models.ProjectSettings.objects.return_value = [
        models.ProjectSettings(
            project=_project_document(project_id), settings=settings_document
        )
        for project_id in ["P2", "P1", "P2"]
    ]

    result = settings_backend.assigned_project_ids("fallback_v7")

    assert result == ["P1", "P2"]
    mock_models.Settings.objects.assert_called_once_with(name="fallback", version=7)
    mock_models.ProjectSettings.objects.assert_called_once_with(
        settings=settings_document
    )


def test_backend_assigned_project_ids_of_unknown_settings_raises(
    mock_models: MagicMock,
) -> None:
    """Unknown settings are an error."""
    mock_models.Settings.objects.return_value.first.return_value = None

    with pytest.raises(NotFoundError, match="fallback_v7"):
        settings_backend.assigned_project_ids("fallback_v7")


def test_find_project_settings_skips_assignment_with_missing_settings(
    mock_models: MagicMock,
) -> None:
    """An assignment whose settings no longer exist is left out, the others are returned."""
    dangling = models.ProjectSettings(
        id=ObjectId(),
        project=_project_document("P1"),
        settings=models.Settings(id=ObjectId()),
        created_at_=NAIVE_TIME,
    )
    intact = models.ProjectSettings(
        id=ASSIGNMENT_OBJECT_ID,
        project=_project_document("P1"),
        settings=_settings_document(),
        created_at_=NAIVE_TIME,
    )
    mock_models.ProjectSettings.objects.return_value.__iter__.return_value = [
        dangling,
        intact,
    ]
    mock_models.Settings.objects.return_value = [_settings_document()]

    # when
    result = project_settings.find(["P1"])

    assert [assignment.id for assignment in result] == [str(ASSIGNMENT_OBJECT_ID)]
