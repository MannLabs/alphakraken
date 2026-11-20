"""Tests for the write routes of the projects, the API key and the error mapping."""

from collections.abc import Iterator
from unittest.mock import MagicMock, patch

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from rest_api.auth import API_KEY_ENV_VAR, API_KEY_HEADER
from rest_api.main import app
from rest_api.service import ProjectWithAssignments
from shared.domain.errors import AlreadyExistsError, InvalidInputError, NotFoundError
from shared.keys import FALLBACK_PROJECT_ID
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


# differs from the request in every field, to show that the response is what was read back
STORED_PROJECT = project(
    id="P123",
    name="stored name",
    description="stored description",
    owner_email="stored@example.org",
)


@pytest.fixture
def mock_get_project() -> Iterator[MagicMock]:
    """The read of the created project."""
    with patch(
        "rest_api.routers.projects.get_project",
        return_value=ProjectWithAssignments(STORED_PROJECT, []),
    ) as get_project:
        yield get_project


@pytest.fixture
def mock_add(mock_get_project: MagicMock) -> Iterator[MagicMock]:  # noqa: ARG001
    """The write function."""
    with patch("rest_api.routers.projects.project_store.add") as add:
        yield add


def test_create_project(mock_add: MagicMock, mock_get_project: MagicMock) -> None:
    """A project is added with every field passed by name, then read back and returned as stored."""
    response = client.post("/projects/", json=NEW_PROJECT, headers=KEY_HEADERS)

    assert response.status_code == status.HTTP_201_CREATED
    assert response.json() == {
        "id": "P123",
        "name": "stored name",
        "description": "stored description",
        "status": "active",
        "owner_email": "stored@example.org",
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
    mock_get_project.assert_called_once_with("P123")


@pytest.mark.parametrize(
    "content", [b"{not json", b'{"project_id": 1}', b"x" * 100_000]
)
def test_write_without_key_is_rejected_before_the_body_is_checked(
    mock_add: MagicMock, content: bytes
) -> None:
    """Without the key the answer is 401 whatever the body, not a 422 about the body."""
    response = client.post(
        "/projects/", content=content, headers={"Content-Type": "application/json"}
    )

    assert response.status_code == status.HTTP_401_UNAUTHORIZED
    assert response.json() == {"detail": "Invalid or missing API key"}
    mock_add.assert_not_called()


def test_write_to_unknown_path_without_key_is_401() -> None:
    """The key is checked for every write, before the route is looked up."""
    assert client.post("/nosuch/").status_code == status.HTTP_401_UNAUTHORIZED
    assert (
        client.post("/nosuch/", headers=KEY_HEADERS).status_code
        == status.HTTP_404_NOT_FOUND
    )


def test_create_project_malformed_json(mock_add: MagicMock) -> None:
    """With the key, a body that is no JSON gives 422 in the common shape."""
    response = client.post(
        "/projects/",
        content=b"{not json",
        headers={**KEY_HEADERS, "Content-Type": "application/json"},
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert response.json() == {
        "detail": [
            "JSON decode error at character 1: Expecting property name enclosed in double quotes"
        ]
    }
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


@pytest.mark.parametrize(
    ("body", "expected"),
    [
        (None, ["Field required"]),
        (
            [NEW_PROJECT],
            ["Input should be a valid dictionary or object to extract fields from"],
        ),
        (
            "text",
            ["Input should be a valid dictionary or object to extract fields from"],
        ),
    ],
)
def test_create_project_body_is_no_object(
    mock_add: MagicMock, body: object, expected: list[str]
) -> None:
    """A missing body, a list or a string gives 422 with the bare message, as no field is at fault."""
    response = client.post("/projects/", json=body, headers=KEY_HEADERS)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert response.json() == {"detail": expected}
    mock_add.assert_not_called()


def test_create_project_field_of_wrong_type(mock_add: MagicMock) -> None:
    """A field of the wrong type is reported like any other violation."""
    response = client.post(
        "/projects/",
        json={**NEW_PROJECT, "name": ["a", "list"], "description": 5},
        headers=KEY_HEADERS,
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert response.json() == {
        "detail": [
            "name: Input should be a valid string",
            "description: Input should be a valid string",
        ]
    }
    mock_add.assert_not_called()


@patch("shared.db.projects._backend")
def test_create_fallback_project_is_rejected_by_the_write_function(
    mock_backend: MagicMock,
) -> None:
    """With the real write function, the fallback project gives 422 and is not written."""
    mock_backend.find.return_value = []

    response = client.post(
        "/projects/",
        json={"project_id": FALLBACK_PROJECT_ID, "name": "Fallback"},
        headers=KEY_HEADERS,
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert response.json() == {
        "detail": [f"Creating project '{FALLBACK_PROJECT_ID}' is not allowed."]
    }
    mock_backend.add.assert_not_called()


def test_create_project_misspelled_key(mock_add: MagicMock) -> None:
    """A misspelled key is rejected, not dropped."""
    response = client.post(
        "/projects/", json={**NEW_PROJECT, "descriptio": "x"}, headers=KEY_HEADERS
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert response.json() == {"detail": ["descriptio: Extra inputs are not permitted"]}
    mock_add.assert_not_called()


WRITES_BY_ID = [
    ("post", "/projects/P123/archive", "archive"),
    ("delete", "/projects/P123", "delete"),
]


@pytest.mark.parametrize(("method", "path", "function"), WRITES_BY_ID)
def test_archive_and_delete_project(method: str, path: str, function: str) -> None:
    """Archive and delete call their write function and return no content."""
    with patch(f"rest_api.routers.projects.project_store.{function}") as mock_write:
        response = client.request(method, path, headers=KEY_HEADERS)

    assert response.status_code == status.HTTP_204_NO_CONTENT
    assert response.content == b""
    mock_write.assert_called_once_with("P123")


@pytest.mark.parametrize(("method", "path", "function"), WRITES_BY_ID)
@pytest.mark.parametrize(
    ("error", "status_code", "body"),
    [
        (
            NotFoundError("No project P123"),
            status.HTTP_404_NOT_FOUND,
            {"detail": "No project P123"},
        ),
        (
            InvalidInputError(["already archived"]),
            status.HTTP_422_UNPROCESSABLE_CONTENT,
            {"detail": ["already archived"]},
        ),
    ],
)
def test_archive_and_delete_project_errors(  # noqa: PLR0913
    method: str,
    path: str,
    function: str,
    error: Exception,
    status_code: int,
    body: dict,
) -> None:
    """The errors of the write functions are mapped to their status codes."""
    with patch(
        f"rest_api.routers.projects.project_store.{function}", side_effect=error
    ):
        response = client.request(method, path, headers=KEY_HEADERS)

    assert response.status_code == status_code
    assert response.json() == body


def test_invalid_query_parameter_names_its_location() -> None:
    """An error outside the body keeps its location."""
    response = client.get("/projects/", params={"limit": 0})

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert response.json() == {
        "detail": ["query.limit: Input should be greater than or equal to 1"]
    }
