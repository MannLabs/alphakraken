"""Tests for the write routes of the assignments."""

from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from rest_api.auth import API_KEY_ENV_VAR, API_KEY_HEADER
from rest_api.main import app
from shared.domain.errors import InvalidInputError, NotFoundError
from shared.tests.factories import project_settings

client = TestClient(app)

API_KEY = "some-key"
KEY_HEADERS = {API_KEY_HEADER: API_KEY}
NEW_ASSIGNMENT = {
    "project_id": "P1",
    "settings_id": "fallback_v7",
    "scopes": ["thermo"],
    "excluded_scopes": ["astral1"],
    "raw_file_id_filter": ["hela"],
    "raw_file_id_exclude_filter": ["blank"],
}
ASSIGNMENT = project_settings(
    scopes=["thermo"],
    excluded_scopes=["astral1"],
    raw_file_id_filter=["hela"],
    raw_file_id_exclude_filter=["blank"],
)
ASSIGNMENT_JSON = {
    "id": "assignment1",
    "project_id": "P1",
    "settings_id": "fallback_v7",
    "name": "fallback",
    "version": 7,
    "scopes": ["thermo"],
    "excluded_scopes": ["astral1"],
    "raw_file_id_filter": ["hela"],
    "raw_file_id_exclude_filter": ["blank"],
}


@pytest.fixture(autouse=True)
def api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Configure the API key."""
    monkeypatch.setenv(API_KEY_ENV_VAR, API_KEY)


@pytest.fixture
def mock_assign() -> Iterator[MagicMock]:
    """The write function, returning the created assignment."""
    with patch(
        "rest_api.routers.assignments.project_settings_store.assign",
        return_value=ASSIGNMENT,
    ) as assign:
        yield assign


def test_create_assignment(mock_assign: MagicMock) -> None:
    """An assignment is created with every field passed by name, never for the fallback project."""
    response = client.post("/assignments/", json=NEW_ASSIGNMENT, headers=KEY_HEADERS)

    assert response.status_code == status.HTTP_201_CREATED
    assert response.json() == ASSIGNMENT_JSON
    mock_assign.assert_called_once_with(**NEW_ASSIGNMENT, allow_fallback=False)


def test_create_assignment_invalid_input(mock_assign: MagicMock) -> None:
    """All violations found by the write function are listed in one 422."""
    mock_assign.side_effect = InvalidInputError(["unknown scope", "unknown settings"])

    response = client.post("/assignments/", json=NEW_ASSIGNMENT, headers=KEY_HEADERS)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert response.json() == {"detail": ["unknown scope", "unknown settings"]}


@pytest.mark.parametrize(
    "missing",
    ["scopes", "excluded_scopes", "raw_file_id_filter", "raw_file_id_exclude_filter"],
)
def test_create_assignment_requires_all_lists(
    mock_assign: MagicMock, missing: str
) -> None:
    """None of the four lists has a default."""
    body = {k: v for k, v in NEW_ASSIGNMENT.items() if k != missing}

    response = client.post("/assignments/", json=body, headers=KEY_HEADERS)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert response.json() == {"detail": [f"{missing}: Field required"]}
    mock_assign.assert_not_called()


def test_create_assignment_misspelled_key(mock_assign: MagicMock) -> None:
    """A misspelled key is rejected, not dropped."""
    response = client.post(
        "/assignments/",
        json={**NEW_ASSIGNMENT, "excluded_scope": ["astral2"]},
        headers=KEY_HEADERS,
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert response.json() == {
        "detail": ["excluded_scope: Extra inputs are not permitted"]
    }
    mock_assign.assert_not_called()


def test_delete_assignment() -> None:
    """An assignment is removed, never one of the fallback project."""
    with patch(
        "rest_api.routers.assignments.project_settings_store.unassign"
    ) as mock_unassign:
        response = client.delete("/assignments/assignment1", headers=KEY_HEADERS)

    assert response.status_code == status.HTTP_204_NO_CONTENT
    assert response.content == b""
    mock_unassign.assert_called_once_with("assignment1", allow_fallback=False)


def test_update_assignment_to_latest() -> None:
    """The updated assignment is returned, never one of the fallback project."""
    with patch(
        "rest_api.routers.assignments.project_settings_store.update_to_latest",
        return_value=ASSIGNMENT,
    ) as mock_update:
        response = client.post(
            "/assignments/assignment1/update_to_latest", headers=KEY_HEADERS
        )

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == ASSIGNMENT_JSON
    mock_update.assert_called_once_with("assignment1", allow_fallback=False)


WRITES_BY_ID = [
    ("delete", "/assignments/assignment1", "unassign"),
    ("post", "/assignments/assignment1/update_to_latest", "update_to_latest"),
]


@pytest.mark.parametrize(("method", "path", "function"), WRITES_BY_ID)
@pytest.mark.parametrize(
    ("error", "status_code", "body"),
    [
        (
            NotFoundError("No settings assignment assignment1"),
            status.HTTP_404_NOT_FOUND,
            {"detail": "No settings assignment assignment1"},
        ),
        (
            InvalidInputError(["not allowed for the fallback project"]),
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            {"detail": ["not allowed for the fallback project"]},
        ),
    ],
)
def test_assignment_writes_by_id_errors(  # noqa: PLR0913
    method: str,
    path: str,
    function: str,
    error: Exception,
    status_code: int,
    body: dict,
) -> None:
    """The errors of the write functions are mapped to their status codes."""
    with patch(
        f"rest_api.routers.assignments.project_settings_store.{function}",
        side_effect=error,
    ):
        response = client.request(method, path, headers=KEY_HEADERS)

    assert response.status_code == status_code
    assert response.json() == body
