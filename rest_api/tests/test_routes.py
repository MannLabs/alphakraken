"""Tests for the routes of the REST API."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from rest_api.main import app
from rest_api.service import ProjectWithAssignments, RawFileWithMetrics
from shared.domain.errors import NotFoundError
from shared.tests.factories import project, project_settings, raw_file, settings

client = TestClient(app)

SOME_TIME_JSON = "2024-01-01T12:00:00Z"

PROJECT = ProjectWithAssignments(project(), [project_settings()])
PROJECT_JSON = {
    "id": "P1",
    "name": "Project 1",
    "description": "desc",
    "status": "active",
    "owner_email": None,
    "created_at_": SOME_TIME_JSON,
    "settings": [
        {
            "settings_id": "fallback_v7",
            "name": "fallback",
            "version": 7,
            "scopes": ["*"],
            "excluded_scopes": [],
            "raw_file_id_filter": [],
            "raw_file_id_exclude_filter": [],
        }
    ],
}

SETTINGS = settings(fasta_file_name="human.fasta", owner_email="masc@example.org")
SETTINGS_JSON = {
    "id": "fallback_v7",
    "number": 1,
    "name": "fallback",
    "version": 7,
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
    "slurm_mem": "",
    "slurm_time": "",
    "num_threads": 1,
    "status": "active",
    "owner_email": "masc@example.org",
    "created_at_": SOME_TIME_JSON,
}

RAW_FILE_JSON = {
    "id": "file.raw",
    "collision_flag": None,
    "original_name": "file.raw",
    "instrument_id": "inst1",
    "project_id": "_FALLBACK",
    "status": "done",
    "status_details": None,
    "size": 1024,
    "backup_status": None,
    "backup_display_base_path": None,
    "s3_upload_path": None,
    "instrument_file_status": "n/a",
    "created_at": SOME_TIME_JSON,
    "finalized_at": None,
    "updated_at_": SOME_TIME_JSON,
    "metrics": None,
    "file_info": None,
    "backup_path": None,
}


@patch("rest_api.routes.get_raw_files_with_metrics")
def test_list_raw_files_without_joined_data(mock_get_raw_files: MagicMock) -> None:
    """Every declared field is present, null when unset or not requested."""
    mock_get_raw_files.return_value = (
        [RawFileWithMetrics(raw_file(), metrics=None, backup_path=None)],
        1,
    )

    response = client.get("/raw_files/", params={"include_metrics": False})

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {
        "total": 1,
        "offset": 0,
        "limit": 100,
        "data": [RAW_FILE_JSON],
    }
    mock_get_raw_files.assert_called_once_with(
        instrument_id=None,
        name_contains=None,
        project_id=None,
        date_from=None,
        date_to=None,
        limit=100,
        offset=0,
        include_metrics=False,
        include_file_info=False,
    )


@patch("rest_api.routes.get_raw_files_with_metrics")
def test_list_raw_files_with_joined_data(mock_get_raw_files: MagicMock) -> None:
    """Metrics, file info and backup path are returned with the raw file."""
    mock_get_raw_files.return_value = (
        [
            RawFileWithMetrics(
                raw_file(file_info={"a.raw": (1.0, "hash")}),
                metrics=[{"type": "alphadia", "proteins": 5000}],
                backup_path="/backup/f1",
            )
        ],
        1,
    )

    response = client.get("/raw_files/", params={"include_file_info": True})

    assert response.json()["data"] == [
        {
            **RAW_FILE_JSON,
            "metrics": [{"type": "alphadia", "proteins": 5000}],
            "file_info": {"a.raw": [1.0, "hash"]},
            "backup_path": "/backup/f1",
        }
    ]


@patch("rest_api.routes.get_projects")
def test_list_projects(mock_get_projects: MagicMock) -> None:
    """The list is wrapped in the pagination envelope and filters are passed on."""
    mock_get_projects.return_value = ([PROJECT], 1)

    response = client.get("/projects/", params={"project_id": "P1", "limit": 5})

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == {
        "total": 1,
        "offset": 0,
        "limit": 5,
        "data": [PROJECT_JSON],
    }
    mock_get_projects.assert_called_once_with(project_id="P1", limit=5, offset=0)


@pytest.mark.parametrize("path", ["/raw_files/", "/projects/", "/settings/"])
@pytest.mark.parametrize("params", [{"limit": 0}, {"limit": 1001}, {"offset": -1}])
def test_lists_reject_invalid_pagination(path: str, params: dict[str, int]) -> None:
    """Pagination bounds are validated."""
    response = client.get(path, params=params)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT


@patch("rest_api.routes.get_project")
def test_read_project(mock_get_project: MagicMock) -> None:
    """A single project is returned by ID."""
    mock_get_project.return_value = PROJECT

    response = client.get("/projects/P1")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == PROJECT_JSON
    mock_get_project.assert_called_once_with("P1")


@patch("rest_api.routes.get_project", side_effect=NotFoundError("No project nope"))
def test_read_project_not_found(mock_get_project: MagicMock) -> None:
    """Unknown project IDs yield 404 with the message of the error."""
    response = client.get("/projects/nope")

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json() == {"detail": "No project nope"}
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
        "data": [SETTINGS_JSON],
    }
    mock_get_settings.assert_called_once_with(name="fallback", limit=100, offset=2)


@patch("rest_api.routes.get_settings_by_unique_name")
def test_read_settings(mock_get_settings_by_unique_name: MagicMock) -> None:
    """A single settings entry is returned by ID."""
    mock_get_settings_by_unique_name.return_value = SETTINGS

    response = client.get("/settings/fallback_v7")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == SETTINGS_JSON


@patch(
    "rest_api.routes.get_settings_by_unique_name",
    side_effect=NotFoundError("No settings nope"),
)
def test_read_settings_not_found(mock_get_settings_by_unique_name: MagicMock) -> None:
    """Unknown settings IDs yield 404 with the message of the error."""
    response = client.get("/settings/nope")

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json() == {"detail": "No settings nope"}
    mock_get_settings_by_unique_name.assert_called_once_with("nope")
