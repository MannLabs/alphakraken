"""Tests for the write routes of the settings."""

from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from rest_api.auth import API_KEY_ENV_VAR, API_KEY_HEADER
from rest_api.main import app
from shared.domain.errors import AlreadyExistsError, InvalidInputError, NotFoundError
from shared.tests.factories import settings

client = TestClient(app)

API_KEY = "some-key"
KEY_HEADERS = {API_KEY_HEADER: API_KEY}
NEW_SETTINGS = {
    "name": "fallback",
    "software_type": "alphadia",
    "software": "alphadia-1.0",
    "runner_name": "slurm",
    "metrics_type": "alphadia",
    "description": "desc",
    "fasta_file_name": "human.fasta",
    "speclib_file_name": "lib.speclib",
    "config_file_name": "config.yaml",
    "config_params": None,
    "slurm_cpus_per_task": 4,
    "slurm_mem": "8G",
    "slurm_time": "02:00:00",
    "num_threads": 4,
    "owner_email": "masc@example.org",
}
MINIMAL_SETTINGS = {
    "name": "fallback",
    "software_type": "msqc",
    "software": "msqc-1.0",
    "runner_name": "slurm",
    "metrics_type": "msqc",
}


@pytest.fixture(autouse=True)
def api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Configure the API key."""
    monkeypatch.setenv(API_KEY_ENV_VAR, API_KEY)


@pytest.fixture
def mock_create() -> Iterator[MagicMock]:
    """The write function, returning the created settings."""
    with patch(
        "rest_api.routers.settings.settings_store.create",
        return_value=settings(description="as stored"),
    ) as create:
        yield create


def test_create_settings(mock_create: MagicMock) -> None:
    """Settings are created with every field passed by name and returned as they were stored."""
    response = client.post("/settings/", json=NEW_SETTINGS, headers=KEY_HEADERS)

    assert response.status_code == status.HTTP_201_CREATED
    assert response.json()["id"] == "fallback_v7"
    assert response.json()["description"] == "as stored"
    mock_create.assert_called_once_with(**NEW_SETTINGS, archive_previous=False)


def test_create_settings_archiving_the_previous_version(mock_create: MagicMock) -> None:
    """The query parameter is passed on."""
    response = client.post(
        "/settings/",
        params={"archive_previous": True},
        json=NEW_SETTINGS,
        headers=KEY_HEADERS,
    )

    assert response.status_code == status.HTTP_201_CREATED
    assert mock_create.call_args.kwargs["archive_previous"] is True


def test_create_settings_fills_resource_defaults(mock_create: MagicMock) -> None:
    """Missing resource parameters are passed on as the defaults of the software type."""
    response = client.post("/settings/", json=MINIMAL_SETTINGS, headers=KEY_HEADERS)

    assert response.status_code == status.HTTP_201_CREATED
    passed = mock_create.call_args.kwargs
    assert passed["slurm_cpus_per_task"] is not None
    assert passed["slurm_mem"]
    assert passed["description"] is None


@pytest.mark.parametrize(
    ("error", "status_code", "body"),
    [
        (
            InvalidInputError(["Unknown runner 'nosuch'."]),
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            {"detail": ["Unknown runner 'nosuch'."]},
        ),
        (
            AlreadyExistsError("version taken"),
            status.HTTP_409_CONFLICT,
            {"detail": "version taken"},
        ),
    ],
)
def test_create_settings_errors(
    mock_create: MagicMock, error: Exception, status_code: int, body: dict
) -> None:
    """The errors of the write function are mapped to their status codes."""
    mock_create.side_effect = error

    response = client.post("/settings/", json=NEW_SETTINGS, headers=KEY_HEADERS)

    assert response.status_code == status_code
    assert response.json() == body


def test_create_settings_invalid_body(mock_create: MagicMock) -> None:
    """Every violation of the body is reported, the field first."""
    response = client.post(
        "/settings/",
        json={**MINIMAL_SETTINGS, "name": "no spaces", "slurm_mem": "8 GB"},
        headers=KEY_HEADERS,
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert response.json() == {
        "detail": [
            "name: may only contain letters, digits and underscores.",
            "slurm_mem: Memory must be a number followed by a unit (K, M, G, or T), e.g. '62G'.",
        ]
    }
    mock_create.assert_not_called()


def test_create_settings_misspelled_key(mock_create: MagicMock) -> None:
    """A misspelled key is rejected, not dropped."""
    response = client.post(
        "/settings/", json={**MINIMAL_SETTINGS, "fasta_file": "x"}, headers=KEY_HEADERS
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert response.json() == {"detail": ["fasta_file: Extra inputs are not permitted"]}
    mock_create.assert_not_called()


WRITES_BY_ID = [
    ("post", "/settings/fallback_v7/archive", "archive"),
    ("delete", "/settings/fallback_v7", "delete"),
]


@pytest.mark.parametrize(("method", "path", "function"), WRITES_BY_ID)
def test_archive_and_delete_settings(method: str, path: str, function: str) -> None:
    """Archive and delete call their write function and return no content."""
    with patch(f"rest_api.routers.settings.settings_store.{function}") as mock_write:
        response = client.request(method, path, headers=KEY_HEADERS)

    assert response.status_code == status.HTTP_204_NO_CONTENT
    assert response.content == b""
    mock_write.assert_called_once_with("fallback_v7")


@pytest.mark.parametrize(("method", "path", "function"), WRITES_BY_ID)
@pytest.mark.parametrize(
    ("error", "status_code", "body"),
    [
        (
            NotFoundError("No settings fallback_v7"),
            status.HTTP_404_NOT_FOUND,
            {"detail": "No settings fallback_v7"},
        ),
        (
            InvalidInputError(["still assigned to projects P1"]),
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            {"detail": ["still assigned to projects P1"]},
        ),
    ],
)
def test_archive_and_delete_settings_errors(  # noqa: PLR0913
    method: str,
    path: str,
    function: str,
    error: Exception,
    status_code: int,
    body: dict,
) -> None:
    """The errors of the write functions are mapped to their status codes."""
    with patch(
        f"rest_api.routers.settings.settings_store.{function}", side_effect=error
    ):
        response = client.request(method, path, headers=KEY_HEADERS)

    assert response.status_code == status_code
    assert response.json() == body
