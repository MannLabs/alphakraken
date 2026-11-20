"""Tests for the project and settings queries of the REST API service."""

from unittest.mock import MagicMock, patch

import pytest

from rest_api.service import (
    ProjectWithAssignments,
    get_project,
    get_projects,
    get_settings,
    get_settings_by_unique_name,
)
from shared.domain.errors import NotFoundError
from shared.domain.settings import SettingsStatus
from shared.tests.factories import project, project_settings, settings


@patch("shared.db.project_settings.find")
@patch("shared.db.projects.count", return_value=7)
@patch("shared.db.projects.find")
def test_get_projects_groups_assignments_by_project(
    mock_find: MagicMock, mock_count: MagicMock, mock_find_assignments: MagicMock
) -> None:
    """Projects carry their own assignments; projects without assignments get an empty list."""
    p1, p2 = project(id="P1"), project(id="P2")
    assignment = project_settings(project_id="P1")
    mock_find.return_value = [p1, p2]
    mock_find_assignments.return_value = [assignment]

    data, total = get_projects(limit=2, offset=4)

    assert total == 7
    assert data == [
        ProjectWithAssignments(p1, [assignment]),
        ProjectWithAssignments(p2, []),
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
    assert [p.project.id for p in data] == ["P1"]
    mock_find.assert_called_once_with(ids=["P1"], limit=100, offset=0)
    mock_count.assert_called_once_with(ids=["P1"])


@patch("rest_api.service.get_projects")
def test_get_project_returns_first_match(mock_get_projects: MagicMock) -> None:
    """A single project is looked up by ID."""
    found = ProjectWithAssignments(project(), [])
    mock_get_projects.return_value = ([found], 1)

    assert get_project("P1") is found
    mock_get_projects.assert_called_once_with(project_id="P1", limit=1)


@patch("rest_api.service.get_projects")
def test_get_project_raises_if_missing(mock_get_projects: MagicMock) -> None:
    """Unknown IDs, and deleted projects, which the query leaves out, are not found."""
    mock_get_projects.return_value = ([], 0)

    with pytest.raises(NotFoundError, match="P1"):
        get_project("P1")


@patch("shared.db.settings.count", return_value=3)
@patch("shared.db.settings.find")
def test_get_settings(mock_find: MagicMock, mock_count: MagicMock) -> None:
    """The filter and the page are passed on."""
    mock_find.return_value = [settings()]

    data, total = get_settings(name="fallback", limit=10, offset=20)

    assert total == 3
    assert data == [settings()]
    mock_find.assert_called_once_with(name="fallback", limit=10, offset=20)
    mock_count.assert_called_once_with(name="fallback")


@patch("shared.db.settings.get")
def test_get_settings_by_unique_name(mock_get: MagicMock) -> None:
    """Settings are looked up by their unique name."""
    mock_get.return_value = settings()

    assert get_settings_by_unique_name("fallback_v7") == settings()
    mock_get.assert_called_once_with("fallback_v7")


@patch("shared.db.settings.get", side_effect=NotFoundError("none"))
def test_get_settings_by_unique_name_raises_if_missing(
    mock_get: MagicMock,  # noqa: ARG001
) -> None:
    """The error for unknown or malformed names is passed on."""
    with pytest.raises(NotFoundError, match="none"):
        get_settings_by_unique_name("fallback_v99")


@patch("shared.db.settings.get")
def test_get_settings_by_unique_name_raises_for_deleted_settings(
    mock_get: MagicMock,
) -> None:
    """Deleted settings are not found."""
    mock_get.return_value = settings(status=SettingsStatus.DELETED)

    with pytest.raises(NotFoundError, match="fallback_v7"):
        get_settings_by_unique_name("fallback_v7")
