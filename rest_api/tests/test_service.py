"""Tests for the project and settings queries of the REST API service."""

from unittest.mock import MagicMock, patch

from rest_api.service import (
    get_project,
    get_projects,
    get_settings,
    get_settings_by_unique_name,
)
from shared.domain.errors import NotFoundError
from shared.domain.projects import ProjectStatus
from shared.domain.settings import SettingsStatus
from shared.tests.factories import SOME_TIME, project, project_settings, settings


@patch("shared.db.project_settings.find")
@patch("shared.db.projects.count", return_value=7)
@patch("shared.db.projects.find")
def test_get_projects_groups_assignments_by_project(
    mock_find: MagicMock, mock_count: MagicMock, mock_find_assignments: MagicMock
) -> None:
    """Projects carry their own assignments; projects without assignments get an empty list."""
    mock_find.return_value = [project(id="P1"), project(id="P2")]
    mock_find_assignments.return_value = [
        project_settings(
            project_id="P1",
            scopes=["test1"],
            raw_file_id_filter=["hela"],
            raw_file_id_exclude_filter=["blank"],
        )
    ]

    data, total = get_projects(limit=2, offset=4)

    assert total == 7
    assert data == [
        {
            "id": "P1",
            "name": "Project 1",
            "description": "desc",
            "status": ProjectStatus.ACTIVE,
            "created_at_": SOME_TIME,
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
            "created_at_": SOME_TIME,
            "settings": [],
        },
    ]
    mock_find.assert_called_once_with(ids=None, limit=2, offset=4)
    mock_count.assert_called_once_with(ids=None)
    mock_find_assignments.assert_called_once_with(["P1", "P2"])


@patch("shared.db.project_settings.find", return_value=[])
@patch("shared.db.projects.count", return_value=1)
@patch("shared.db.projects.find")
def test_get_projects_filters_by_id(
    mock_find: MagicMock,
    mock_count: MagicMock,
    mock_find_assignments: MagicMock,  # noqa: ARG001
) -> None:
    """The project_id filter is applied to the query and the count."""
    mock_find.return_value = [project(id="P1")]

    data, total = get_projects(project_id="P1")

    assert total == 1
    assert [p["id"] for p in data] == ["P1"]
    mock_find.assert_called_once_with(ids=["P1"], limit=100, offset=0)
    mock_count.assert_called_once_with(ids=["P1"])


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


@patch("shared.db.settings.count", return_value=3)
@patch("shared.db.settings.find")
def test_get_settings_returns_all_fields(
    mock_find: MagicMock, mock_count: MagicMock
) -> None:
    """Settings are returned with the unique name as id and all set fields."""
    mock_find.return_value = [settings()]

    data, total = get_settings(name="fallback", limit=10, offset=20)

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
            "created_at_": SOME_TIME,
        }
    ]
    mock_find.assert_called_once_with(name="fallback", limit=10, offset=20)
    mock_count.assert_called_once_with(name="fallback")


@patch("shared.db.settings.get")
def test_get_settings_by_unique_name(mock_get: MagicMock) -> None:
    """Settings are looked up by their unique name."""
    mock_get.return_value = settings()

    result = get_settings_by_unique_name("fallback_v7")

    assert result is not None
    assert result["id"] == "fallback_v7"
    mock_get.assert_called_once_with("fallback_v7")


@patch("shared.db.settings.get", side_effect=NotFoundError("none"))
def test_get_settings_by_unique_name_returns_none_if_missing(
    mock_get: MagicMock,  # noqa: ARG001
) -> None:
    """None is returned for unknown or malformed names."""
    assert get_settings_by_unique_name("fallback_v99") is None


@patch("shared.db.settings.get")
def test_get_settings_by_unique_name_returns_none_for_deleted_settings(
    mock_get: MagicMock,
) -> None:
    """Deleted settings are not served."""
    mock_get.return_value = settings(status=SettingsStatus.DELETED)

    assert get_settings_by_unique_name("fallback_v7") is None
