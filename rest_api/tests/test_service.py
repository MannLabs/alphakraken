"""Tests for the project and settings queries of the REST API service."""

from datetime import datetime
from unittest.mock import MagicMock, patch

from rest_api.service import (
    get_project,
    get_projects,
    get_settings,
    get_settings_by_unique_name,
)
from shared.db.models import (
    Project,
    ProjectSettings,
    ProjectStatus,
    Settings,
    SettingsStatus,
)

CREATED_AT = datetime(2025, 1, 2, 3, 4, 5)  # noqa: DTZ001


def _settings(name: str = "fallback", version: int = 7) -> Settings:
    return Settings(
        number=1,
        name=name,
        version=version,
        software_type="alphadia",
        software="alphadia-1.0",
        runner_name="slurm",
        metrics_type="alphadia",
        created_at_=CREATED_AT,
    )


def _project(project_id: str = "P1") -> Project:
    return Project(
        id=project_id, name="Project 1", description="desc", created_at_=CREATED_AT
    )


def _mock_query(mock_model: MagicMock, docs: list, total: int) -> MagicMock:
    query = mock_model.objects.return_value
    query.filter.return_value = query
    query.count.return_value = total
    query.order_by.return_value.skip.return_value.limit.return_value = docs
    return query


@patch("rest_api.service.connect_db")
@patch("rest_api.service.ProjectSettings")
@patch("rest_api.service.Project")
def test_get_projects_groups_assignments_by_project(
    mock_project: MagicMock, mock_ps: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Projects carry their own assignments; projects without assignments get an empty list."""
    p1, p2 = _project("P1"), _project("P2")
    settings = _settings()
    _mock_query(mock_project, [p1, p2], total=7)
    mock_ps.objects.return_value.order_by.return_value = [
        ProjectSettings(
            project=p1,
            settings=settings,
            scopes=["test1"],
            excluded_scopes=[],
            raw_file_id_filter=["hela"],
            raw_file_id_exclude_filter=["blank"],
        )
    ]

    data, total = get_projects(limit=2, offset=4)
    mock_connect_db.assert_called_once()

    assert total == 7
    assert data == [
        {
            "id": "P1",
            "name": "Project 1",
            "description": "desc",
            "status": ProjectStatus.ACTIVE,
            "created_at_": CREATED_AT,
            "settings": [
                {
                    "settings_id": "fallback_v7",
                    "name": "fallback",
                    "version": 7,
                    "scopes": ["test1"],
                    "excluded_scopes": [],
                    "raw_file_id_filter": ["hela"],
                    "raw_file_id_exclude_filter": ["blank"],
                }
            ],
        },
        {
            "id": "P2",
            "name": "Project 1",
            "description": "desc",
            "status": ProjectStatus.ACTIVE,
            "created_at_": CREATED_AT,
            "settings": [],
        },
    ]
    mock_project.objects.assert_called_once_with(status__ne=ProjectStatus.DELETED)
    mock_project.objects.return_value.filter.assert_not_called()
    query = mock_project.objects.return_value.order_by
    query.assert_called_once_with("-created_at_")
    query.return_value.skip.assert_called_once_with(4)
    query.return_value.skip.return_value.limit.assert_called_once_with(2)
    mock_ps.objects.assert_called_once_with(project__in=["P1", "P2"])


@patch("rest_api.service.connect_db")
@patch("rest_api.service.ProjectSettings")
@patch("rest_api.service.Project")
def test_get_projects_filters_by_id(
    mock_project: MagicMock, mock_ps: MagicMock, mock_connect_db: MagicMock
) -> None:
    """The project_id filter is applied to the query."""
    query = _mock_query(mock_project, [_project("P1")], total=1)
    mock_ps.objects.return_value.order_by.return_value = []

    data, total = get_projects(project_id="P1")
    mock_connect_db.assert_called_once()

    assert total == 1
    assert [p["id"] for p in data] == ["P1"]
    query.filter.assert_called_once_with(id="P1")


@patch("rest_api.service.get_projects")
def test_get_project_returns_first_match(mock_get_projects: MagicMock) -> None:
    """A single project is looked up by ID."""
    mock_get_projects.return_value = ([{"id": "P1"}], 1)

    assert get_project("P1") == {"id": "P1"}
    mock_get_projects.assert_called_once_with(project_id="P1", limit=1)


@patch("rest_api.service.get_projects")
def test_get_project_returns_none_if_missing(mock_get_projects: MagicMock) -> None:
    """None is returned for unknown IDs."""
    mock_get_projects.return_value = ([], 0)

    assert get_project("P1") is None


@patch("rest_api.service.connect_db")
@patch("rest_api.service.Settings")
def test_get_settings_returns_all_fields(
    mock_settings: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Settings are returned with a string id and all model fields."""
    settings = _settings()
    query = _mock_query(mock_settings, [settings], total=3)

    data, total = get_settings(name="fallback", limit=10, offset=20)
    mock_connect_db.assert_called_once()

    assert total == 3
    assert data == [
        {
            "id": "fallback_v7",
            "number": 1,
            "name": "fallback",
            "version": 7,
            "software_type": "alphadia",
            "software": "alphadia-1.0",
            "runner_name": "slurm",
            "metrics_type": "alphadia",
            "slurm_cpus_per_task": 1,
            "slurm_mem": "",
            "slurm_time": "",
            "num_threads": 1,
            "status": SettingsStatus.ACTIVE,
            "created_at_": CREATED_AT,
        }
    ]
    mock_settings.objects.assert_called_once_with(status__ne=SettingsStatus.DELETED)
    query.filter.assert_called_once_with(name="fallback")
    query.order_by.assert_called_once_with("-created_at_")
    query.order_by.return_value.skip.assert_called_once_with(20)
    query.order_by.return_value.skip.return_value.limit.assert_called_once_with(10)


@patch("rest_api.service.connect_db")
@patch("rest_api.service.Settings")
def test_get_settings_by_unique_name(
    mock_settings: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Settings are looked up by name and version among non-deleted entries."""
    mock_settings.objects.return_value.first.return_value = _settings()

    result = get_settings_by_unique_name("fallback_v7")
    mock_connect_db.assert_called_once()

    assert result is not None
    assert result["id"] == "fallback_v7"
    mock_settings.objects.assert_called_once_with(
        name="fallback", version=7, status__ne=SettingsStatus.DELETED
    )


@patch("rest_api.service.connect_db")
@patch("rest_api.service.Settings")
def test_get_settings_by_unique_name_returns_none_if_missing(
    mock_settings: MagicMock, mock_connect_db: MagicMock
) -> None:
    """None is returned for unknown names."""
    mock_settings.objects.return_value.first.return_value = None

    assert get_settings_by_unique_name("fallback_v99") is None
    mock_connect_db.assert_called_once()


@patch("rest_api.service.connect_db")
@patch("rest_api.service.Settings")
def test_get_settings_by_unique_name_returns_none_for_invalid_name(
    mock_settings: MagicMock, mock_connect_db: MagicMock
) -> None:
    """A name without version suffix is treated as not found, without querying."""
    assert get_settings_by_unique_name("fallback") is None
    mock_connect_db.assert_called_once()
    mock_settings.objects.assert_not_called()
