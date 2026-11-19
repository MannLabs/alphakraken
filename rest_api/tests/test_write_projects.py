"""Tests for the write routes of the projects, the API key and the error mapping."""

from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from rest_api.auth import API_KEY_ENV_VAR, API_KEY_HEADER
from rest_api.main import app
from rest_api.service import ProjectWithAssignments
from shared.domain.errors import AlreadyExistsError, InvalidInputError
from shared.tests.factories import project

client = TestClient(app)

API_KEY = "some-key"
KEY_HEADERS = {API_KEY_HEADER: API_KEY}
NEW_PROJECT = {
    "project_id": "P123",
    "name": "Project 1",
    "description": "desc",
    "owner_email": "masc@example.org",
}


@pytest.fixture(autouse=True)
def api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Configure the API key."""
    monkeypatch.setenv(API_KEY_ENV_VAR, API_KEY)


@pytest.fixture
def mock_add() -> Iterator[MagicMock]:
    """The write function, and the read of the created project."""
    with (
        patch("rest_api.routers.projects.project_store.add") as add,
        patch(
            "rest_api.routers.projects.get_project",
            return_value=ProjectWithAssignments(
                project(id="P123", owner_email="masc@example.org"), []
            ),
        ),
    ):
        yield add


def test_create_project(mock_add: MagicMock) -> None:
    """A project is added with every field passed by name and returned as it was stored."""
    response = client.post("/projects/", json=NEW_PROJECT, headers=KEY_HEADERS)

    assert response.status_code == status.HTTP_201_CREATED
    assert response.json() == {
        "id": "P123",
        "name": "Project 1",
        "description": "desc",
        "status": "active",
        "owner_email": "masc@example.org",
        "created_at_": "2024-01-01T12:00:00Z",
        "settings": [],
    }
    mock_add.assert_called_once_with(
        project_id="P123",
        name="Project 1",
        description="desc",
        owner_email="masc@example.org",
        allow_fallback=False,
    )


@pytest.mark.parametrize("headers", [{}, {API_KEY_HEADER: "wrong-key"}])
def test_create_project_without_valid_key(
    mock_add: MagicMock, headers: dict[str, str]
) -> None:
    """A missing or wrong key gives 401 and nothing is written."""
    response = client.post("/projects/", json=NEW_PROJECT, headers=headers)

    assert response.status_code == status.HTTP_401_UNAUTHORIZED
    mock_add.assert_not_called()


@pytest.mark.parametrize("configured_key", [None, ""])
def test_create_project_without_configured_key(
    mock_add: MagicMock, monkeypatch: pytest.MonkeyPatch, configured_key: str | None
) -> None:
    """Without a configured key every keyed request gives 401, also one sending an empty key."""
    if configured_key is None:
        monkeypatch.delenv(API_KEY_ENV_VAR)
    else:
        monkeypatch.setenv(API_KEY_ENV_VAR, configured_key)

    for headers in (KEY_HEADERS, {API_KEY_HEADER: ""}, {}):
        response = client.post("/projects/", json=NEW_PROJECT, headers=headers)

        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    mock_add.assert_not_called()


def test_create_project_invalid_input(mock_add: MagicMock) -> None:
    """The violations found by the write function give 422 with one message each."""
    mock_add.side_effect = InvalidInputError(["first", "second"])

    response = client.post("/projects/", json=NEW_PROJECT, headers=KEY_HEADERS)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert response.json() == {"detail": ["first", "second"]}


def test_create_project_already_exists(mock_add: MagicMock) -> None:
    """A project id in use gives 409 with the message of the error."""
    mock_add.side_effect = AlreadyExistsError("Project P123 exists")

    response = client.post("/projects/", json=NEW_PROJECT, headers=KEY_HEADERS)

    assert response.status_code == status.HTTP_409_CONFLICT
    assert response.json() == {"detail": "Project P123 exists"}


def test_create_project_invalid_body(mock_add: MagicMock) -> None:
    """An invalid body gives 422 with messages like the domain ones, the field first."""
    response = client.post(
        "/projects/", json={"project_id": "P123", "name": " "}, headers=KEY_HEADERS
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert response.json() == {"detail": ["name: is required."]}
    mock_add.assert_not_called()


def test_create_project_misspelled_key(mock_add: MagicMock) -> None:
    """A misspelled key is rejected, not dropped."""
    response = client.post(
        "/projects/", json={**NEW_PROJECT, "descriptio": "x"}, headers=KEY_HEADERS
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert response.json() == {"detail": ["descriptio: Extra inputs are not permitted"]}
    mock_add.assert_not_called()


def test_invalid_query_parameter_names_its_location() -> None:
    """An error outside the body keeps its location."""
    response = client.get("/projects/", params={"limit": 0})

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert response.json() == {
        "detail": ["query.limit: Input should be greater than or equal to 1"]
    }
