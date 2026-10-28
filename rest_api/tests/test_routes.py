"""Tests for the project and settings routes of the REST API."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from rest_api.main import app

client = TestClient(app)

PROJECT = {
    "id": "P1",
    "name": "Project 1",
    "description": None,
    "status": "active",
    "created_at_": "2025-01-02T03:04:05",
    "settings": [
        {
            "settings_id": "0123456789abcdef01234567",
            "name": "fallback",
            "version": 1,
            "scopes": ["*"],
            "excluded_scopes": [],
            "raw_file_id_filter": [],
            "raw_file_id_exclude_filter": [],
        }
    ],
}

SETTINGS = {
    "id": "0123456789abcdef01234567",
    "number": 1,
    "name": "fallback",
    "version": 1,
    "description": None,
    "fasta_file_name": "human.fasta",
    "speclib_file_name": None,
    "config_file_name": None,
    "config_params": None,
    "software_type": "alphadia",
    "software": "alphadia-1.0",
    "runner_name": "slurm",
    "metrics_type": "alphadia",
    "slurm_cpus_per_task": 1,
    "slurm_mem": "4G",
    "slurm_time": "01:00:00",
    "num_threads": 1,
    "status": "active",
    "created_at_": "2025-01-02T03:04:05",
}


@patch("rest_api.routes.get_projects")
def test_list_projects(mock_get_projects: MagicMock) -> None:
    """The list is wrapped in the pagination envelope and filters are passed on."""
    mock_get_projects.return_value = ([PROJECT], 1)

    response = client.get("/projects/", params={"project_id": "P1", "limit": 5})

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {"total": 1, "offset": 0, "limit": 5, "data": [PROJECT]}
    mock_get_projects.assert_called_once_with(project_id="P1", limit=5, offset=0)


@pytest.mark.parametrize("params", [{"limit": 0}, {"limit": 1001}, {"offset": -1}])
def test_list_projects_rejects_invalid_pagination(params: dict[str, int]) -> None:
    """Pagination bounds are validated."""
    response = client.get("/projects/", params=params)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@patch("rest_api.routes.get_project")
def test_read_project(mock_get_project: MagicMock) -> None:
    """A single project is returned by ID."""
    mock_get_project.return_value = PROJECT

    response = client.get("/projects/P1")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == PROJECT
    mock_get_project.assert_called_once_with("P1")


@patch("rest_api.routes.get_project", return_value=None)
def test_read_project_not_found(mock_get_project: MagicMock) -> None:
    """Unknown project IDs yield 404."""
    response = client.get("/projects/nope")

    assert response.status_code == status.HTTP_404_NOT_FOUND
    mock_get_project.assert_called_once_with("nope")


@patch("rest_api.routes.get_settings")
def test_list_settings(mock_get_settings: MagicMock) -> None:
    """The list is wrapped in the pagination envelope and filters are passed on."""
    mock_get_settings.return_value = ([SETTINGS], 1)

    response = client.get("/settings/", params={"name": "fallback", "offset": 2})

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {
        "total": 1,
        "offset": 2,
        "limit": 100,
        "data": [SETTINGS],
    }
    mock_get_settings.assert_called_once_with(name="fallback", limit=100, offset=2)


@patch("rest_api.routes.get_settings_by_id")
def test_read_settings(mock_get_settings_by_id: MagicMock) -> None:
    """A single settings entry is returned by ID."""
    mock_get_settings_by_id.return_value = SETTINGS

    response = client.get(f"/settings/{SETTINGS['id']}")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == SETTINGS


@patch("rest_api.routes.get_settings_by_id", return_value=None)
def test_read_settings_not_found(mock_get_settings_by_id: MagicMock) -> None:
    """Unknown settings IDs yield 404."""
    response = client.get("/settings/nope")

    assert response.status_code == status.HTTP_404_NOT_FOUND
    mock_get_settings_by_id.assert_called_once_with("nope")
