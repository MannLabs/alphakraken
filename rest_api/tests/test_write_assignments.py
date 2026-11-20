"""Tests for the write routes of the assignments."""

from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from rest_api.auth import API_KEY_ENV_VAR, API_KEY_HEADER
from rest_api.main import app
from shared.domain.errors import InvalidInputError
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


@pytest.mark.parametrize("headers", [{}, {API_KEY_HEADER: "wrong-key"}])
def test_create_assignment_without_valid_key(
    mock_assign: MagicMock, headers: dict[str, str]
) -> None:
    """A missing or wrong key gives 401 and nothing is written."""
    response = client.post("/assignments/", json=NEW_ASSIGNMENT, headers=headers)

    assert response.status_code == status.HTTP_401_UNAUTHORIZED
    mock_assign.assert_not_called()


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
