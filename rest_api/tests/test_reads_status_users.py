"""Tests for the status and user routes of the REST API."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from rest_api.auth import API_KEY_ENV_VAR, API_KEY_HEADER
from rest_api.main import app
from shared.tests.factories import kraken_status, user

client = TestClient(app)

API_KEY = "some-key"


@pytest.fixture(autouse=True)
def api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Configure the API key."""
    monkeypatch.setenv(API_KEY_ENV_VAR, API_KEY)


@patch("rest_api.routers.status.status_store.find")
def test_list_status(mock_find: MagicMock) -> None:
    """The status of all entities is returned without a key."""
    mock_find.return_value = [kraken_status()]

    response = client.get("/status/")

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == [
        {
            "id": "inst1",
            "status": "ok",
            "status_details": None,
            "free_space_gb": 100,
            "entity_type": "instrument",
            "updated_at_": "2024-01-01T12:00:00Z",
        }
    ]


@patch("rest_api.routers.users.user_store.find")
def test_list_users(mock_find: MagicMock) -> None:
    """Only email and initials of the active users are returned."""
    mock_find.return_value = [user()]

    response = client.get("/users/", headers={API_KEY_HEADER: API_KEY})

    assert response.status_code == status.HTTP_200_OK
    assert response.json() == [{"email": "masc@example.org", "initials": "MaSc"}]
    # inactive users are left out by the default of the query
    mock_find.assert_called_once_with()


@pytest.mark.parametrize("headers", [{}, {API_KEY_HEADER: "wrong-key"}])
@patch("rest_api.routers.users.user_store.find")
def test_list_users_without_valid_key(
    mock_find: MagicMock, headers: dict[str, str]
) -> None:
    """The users are not returned without the key."""
    response = client.get("/users/", headers=headers)

    assert response.status_code == status.HTTP_401_UNAUTHORIZED
    mock_find.assert_not_called()
