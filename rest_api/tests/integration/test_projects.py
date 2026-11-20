"""Integration tests of the project writes: each effect is checked over REST and through `shared.db`."""

from fastapi import status
from fastapi.testclient import TestClient

from shared.db import projects as project_store
from shared.domain.projects import ProjectStatus
from shared.keys import FALLBACK_PROJECT_ID


def _new_project(project_id: str, **fields: str) -> dict[str, str]:
    return {"project_id": project_id, "name": f"integration {project_id}", **fields}


def _exists(project_id: str) -> bool:
    return bool(project_store.find(ids=[project_id], include_deleted=True))


def test_create_project(
    client: TestClient, key_headers: dict[str, str], unique_id: str
) -> None:
    """A created project is returned, readable over REST and active in the database."""
    response = client.post(
        "/projects/",
        json=_new_project(unique_id, description="created by a test"),
        headers=key_headers,
    )

    assert response.status_code == status.HTTP_201_CREATED, response.text
    created = response.json()
    assert created["id"] == unique_id
    assert created["description"] == "created by a test"
    assert created["status"] == "active"
    assert created["settings"] == []
    assert client.get(f"/projects/{unique_id}").json() == created
    assert project_store.get(unique_id).status == ProjectStatus.ACTIVE


def test_create_project_twice_is_409(
    client: TestClient, key_headers: dict[str, str], unique_id: str
) -> None:
    """A project id in use is a conflict."""
    client.post("/projects/", json=_new_project(unique_id), headers=key_headers)

    response = client.post(
        "/projects/", json=_new_project(unique_id), headers=key_headers
    )

    assert response.status_code == status.HTTP_409_CONFLICT
    assert unique_id in response.json()["detail"]


def test_create_fallback_project_is_422(
    client: TestClient, key_headers: dict[str, str]
) -> None:
    """The fallback project cannot be created over REST."""
    response = client.post(
        "/projects/", json=_new_project(FALLBACK_PROJECT_ID), headers=key_headers
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert any(FALLBACK_PROJECT_ID in message for message in response.json()["detail"])


def test_create_project_with_unknown_owner_is_422(
    client: TestClient, key_headers: dict[str, str], unique_id: str
) -> None:
    """An owner who is no user is an invalid input, and nothing is written."""
    response = client.post(
        "/projects/",
        json=_new_project(unique_id, owner_email="nobody@example.org"),
        headers=key_headers,
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert any("nobody@example.org" in message for message in response.json()["detail"])
    assert not _exists(unique_id)


def test_create_project_with_misspelled_key_is_422(
    client: TestClient, key_headers: dict[str, str], unique_id: str
) -> None:
    """A misspelled key is rejected, named without a `body.` prefix, and nothing is written."""
    response = client.post(
        "/projects/",
        json=_new_project(unique_id, descriptio="typo"),
        headers=key_headers,
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert response.json() == {"detail": ["descriptio: Extra inputs are not permitted"]}
    assert not _exists(unique_id)


def test_create_project_without_key_is_401(client: TestClient, unique_id: str) -> None:
    """Without the API key nothing is written."""
    for headers in ({}, {"X-API-Key": "wrong"}):
        response = client.post(
            "/projects/", json=_new_project(unique_id), headers=headers
        )

        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    assert not _exists(unique_id)


def _created(client: TestClient, key_headers: dict[str, str], project_id: str) -> str:
    response = client.post(
        "/projects/", json=_new_project(project_id), headers=key_headers
    )
    assert response.status_code == status.HTTP_201_CREATED, response.text
    return project_id


def test_archive_project(
    client: TestClient, key_headers: dict[str, str], unique_id: str
) -> None:
    """An archived project is inactive; archiving it again is an invalid input."""
    _created(client, key_headers, unique_id)

    response = client.post(f"/projects/{unique_id}/archive", headers=key_headers)

    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
    assert client.get(f"/projects/{unique_id}").json()["status"] == "inactive"
    assert project_store.get(unique_id).status == ProjectStatus.INACTIVE
    again = client.post(f"/projects/{unique_id}/archive", headers=key_headers)
    assert again.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert isinstance(again.json()["detail"], list)


def test_delete_project(
    client: TestClient, key_headers: dict[str, str], unique_id: str
) -> None:
    """A deleted project is no longer returned, but kept; for further writes it is not found."""
    _created(client, key_headers, unique_id)

    response = client.delete(f"/projects/{unique_id}", headers=key_headers)

    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
    assert client.get(f"/projects/{unique_id}").status_code == status.HTTP_404_NOT_FOUND
    assert project_store.get(unique_id).status == ProjectStatus.DELETED
    for again in (
        client.delete(f"/projects/{unique_id}", headers=key_headers),
        client.post(f"/projects/{unique_id}/archive", headers=key_headers),
    ):
        assert again.status_code == status.HTTP_404_NOT_FOUND
        assert again.json() == {"detail": f"No project {unique_id}"}


def test_archive_and_delete_unknown_project_is_404(
    client: TestClient, key_headers: dict[str, str]
) -> None:
    """An unknown project id in the path is not found."""
    for response in (
        client.post("/projects/NOSUCHPROJECT/archive", headers=key_headers),
        client.delete("/projects/NOSUCHPROJECT", headers=key_headers),
    ):
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "NOSUCHPROJECT" in response.json()["detail"]


def test_archive_and_delete_fallback_project_is_422(
    client: TestClient, key_headers: dict[str, str]
) -> None:
    """The fallback project can be neither archived nor deleted."""
    for response in (
        client.post(f"/projects/{FALLBACK_PROJECT_ID}/archive", headers=key_headers),
        client.delete(f"/projects/{FALLBACK_PROJECT_ID}", headers=key_headers),
    ):
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert project_store.get(FALLBACK_PROJECT_ID).status == ProjectStatus.ACTIVE


def test_archive_and_delete_project_without_key_is_401(
    client: TestClient, key_headers: dict[str, str], unique_id: str
) -> None:
    """Without the API key the project stays active."""
    _created(client, key_headers, unique_id)

    for response in (
        client.post(f"/projects/{unique_id}/archive"),
        client.delete(f"/projects/{unique_id}"),
    ):
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    assert project_store.get(unique_id).status == ProjectStatus.ACTIVE


def test_write_without_key_is_401_whatever_the_body(client: TestClient) -> None:
    """The key is checked before the body: a malformed one gives 401, not 422."""
    response = client.post(
        "/projects/", content=b"{not json", headers={"Content-Type": "application/json"}
    )

    assert response.status_code == status.HTTP_401_UNAUTHORIZED
    assert response.json() == {"detail": "Invalid or missing API key"}
