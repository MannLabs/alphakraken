"""Integration tests of the assignment writes; the effects are checked over REST."""

from typing import Any

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from shared.keys import FALLBACK_PROJECT_ID


@pytest.fixture
def project_id(client: TestClient, key_headers: dict[str, str], unique_id: str) -> str:
    """A new active project without assignments."""
    response = client.post(
        "/projects/",
        json={"project_id": unique_id, "name": f"integration {unique_id}"},
        headers=key_headers,
    )
    assert response.status_code == status.HTTP_201_CREATED, response.text
    return unique_id


def _create_settings(client: TestClient, key_headers: dict[str, str], name: str) -> str:
    """Create the next version of the settings and return its id."""
    response = client.post(
        "/settings/",
        json={
            "name": name,
            "software_type": "msqc",
            "software": "msqc-1.0.0",
            "runner_name": "slurm",
            "metrics_type": "msqc",
        },
        headers=key_headers,
    )
    assert response.status_code == status.HTTP_201_CREATED, response.text
    return response.json()["id"]


@pytest.fixture
def settings_id(
    client: TestClient, key_headers: dict[str, str], unique_settings_name: str
) -> str:
    """New active settings, version 1 of `unique_settings_name`."""
    return _create_settings(client, key_headers, unique_settings_name)


def _new_assignment(project_id: str, settings_id: str, **fields: Any) -> dict[str, Any]:
    return {
        "project_id": project_id,
        "settings_id": settings_id,
        "scopes": ["*"],
        "excluded_scopes": [],
        "raw_file_id_filter": [],
        "raw_file_id_exclude_filter": [],
        **fields,
    }


def _assignments(client: TestClient, project_id: str) -> list[dict[str, Any]]:
    return client.get(f"/projects/{project_id}").json()["settings"]


def test_create_assignment(
    client: TestClient, key_headers: dict[str, str], project_id: str, settings_id: str
) -> None:
    """A created assignment is returned with its id and listed with the project."""
    response = client.post(
        "/assignments/",
        json=_new_assignment(
            project_id,
            settings_id,
            scopes=["thermo"],
            excluded_scopes=["bruker"],
            raw_file_id_filter=["hela"],
            raw_file_id_exclude_filter=["blank"],
        ),
        headers=key_headers,
    )

    assert response.status_code == status.HTTP_201_CREATED, response.text
    created = response.json()
    assert created["id"]
    assert created["project_id"] == project_id
    assert created["settings_id"] == settings_id
    assert created["scopes"] == ["thermo"]
    assert created["excluded_scopes"] == ["bruker"]
    assert _assignments(client, project_id) == [created]


def test_create_assignment_twice_is_422(
    client: TestClient, key_headers: dict[str, str], project_id: str, settings_id: str
) -> None:
    """The same settings with the same file name filters cannot be assigned twice."""
    body = _new_assignment(project_id, settings_id)
    client.post("/assignments/", json=body, headers=key_headers)

    response = client.post("/assignments/", json=body, headers=key_headers)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert len(_assignments(client, project_id)) == 1


def test_create_assignment_lists_all_violations(
    client: TestClient, key_headers: dict[str, str]
) -> None:
    """An unknown project, unknown settings and an unknown scope are reported in one 422."""
    response = client.post(
        "/assignments/",
        json=_new_assignment("NOSUCHPROJECT", "nosuch_v1", scopes=["nosuchscope"]),
        headers=key_headers,
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    messages = response.json()["detail"]
    for unknown in ("NOSUCHPROJECT", "nosuch_v1", "nosuchscope"):
        assert any(unknown in message for message in messages), messages


def test_create_assignment_for_fallback_project_is_422(
    client: TestClient, key_headers: dict[str, str], settings_id: str
) -> None:
    """The assignments of the fallback project cannot be changed over REST."""
    before = _assignments(client, FALLBACK_PROJECT_ID)

    response = client.post(
        "/assignments/",
        json=_new_assignment(FALLBACK_PROJECT_ID, settings_id),
        headers=key_headers,
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert _assignments(client, FALLBACK_PROJECT_ID) == before


def test_create_assignment_of_archived_settings_is_422(
    client: TestClient, key_headers: dict[str, str], project_id: str, settings_id: str
) -> None:
    """Archived settings cannot be assigned."""
    client.post(f"/settings/{settings_id}/archive", headers=key_headers)

    response = client.post(
        "/assignments/",
        json=_new_assignment(project_id, settings_id),
        headers=key_headers,
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert _assignments(client, project_id) == []


def test_create_assignment_with_invalid_body_is_422(
    client: TestClient, key_headers: dict[str, str], project_id: str, settings_id: str
) -> None:
    """A missing list and a misspelled key are rejected, named without a `body.` prefix."""
    body = _new_assignment(project_id, settings_id, excluded_scope=["bruker"])
    del body["excluded_scopes"]

    response = client.post("/assignments/", json=body, headers=key_headers)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert sorted(response.json()["detail"]) == [
        "excluded_scope: Extra inputs are not permitted",
        "excluded_scopes: Field required",
    ]
    assert _assignments(client, project_id) == []


def test_create_assignment_without_key_is_401(
    client: TestClient, project_id: str, settings_id: str
) -> None:
    """Without the API key nothing is written."""
    response = client.post(
        "/assignments/", json=_new_assignment(project_id, settings_id)
    )

    assert response.status_code == status.HTTP_401_UNAUTHORIZED
    assert _assignments(client, project_id) == []


# a well-formed id that no assignment has
UNKNOWN_ASSIGNMENT_ID = "0" * 24


@pytest.fixture
def assignment(
    client: TestClient, key_headers: dict[str, str], project_id: str, settings_id: str
) -> dict[str, Any]:
    """A new assignment of `settings_id` to `project_id`, with a scope and a file name filter."""
    response = client.post(
        "/assignments/",
        json=_new_assignment(
            project_id, settings_id, scopes=["thermo"], raw_file_id_filter=["hela"]
        ),
        headers=key_headers,
    )
    assert response.status_code == status.HTTP_201_CREATED, response.text
    return response.json()


def test_delete_assignment(
    client: TestClient, key_headers: dict[str, str], assignment: dict[str, Any]
) -> None:
    """A removed assignment is gone from its project; removing it again is not found."""
    response = client.delete(f"/assignments/{assignment['id']}", headers=key_headers)

    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
    assert _assignments(client, assignment["project_id"]) == []
    again = client.delete(f"/assignments/{assignment['id']}", headers=key_headers)
    assert again.status_code == status.HTTP_404_NOT_FOUND


def test_update_assignment_to_latest(
    client: TestClient,
    key_headers: dict[str, str],
    assignment: dict[str, Any],
    unique_settings_name: str,
) -> None:
    """An assignment is moved to the newest active version, keeping scopes and filters."""
    latest_id = _create_settings(client, key_headers, unique_settings_name)

    response = client.post(
        f"/assignments/{assignment['id']}/update_to_latest", headers=key_headers
    )

    assert response.status_code == status.HTTP_200_OK, response.text
    updated = response.json()
    assert updated == {
        **assignment,
        "settings_id": latest_id,
        "version": assignment["version"] + 1,
    }
    assert _assignments(client, assignment["project_id"]) == [updated]


def test_update_assignment_without_newer_version_is_422(
    client: TestClient, key_headers: dict[str, str], assignment: dict[str, Any]
) -> None:
    """An assignment of the latest version cannot be updated."""
    response = client.post(
        f"/assignments/{assignment['id']}/update_to_latest", headers=key_headers
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert _assignments(client, assignment["project_id"]) == [assignment]


def test_writes_of_unknown_assignment_are_404(
    client: TestClient, key_headers: dict[str, str]
) -> None:
    """An unknown assignment id in the path is not found."""
    for response in (
        client.delete(f"/assignments/{UNKNOWN_ASSIGNMENT_ID}", headers=key_headers),
        client.post(
            f"/assignments/{UNKNOWN_ASSIGNMENT_ID}/update_to_latest",
            headers=key_headers,
        ),
    ):
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert UNKNOWN_ASSIGNMENT_ID in response.json()["detail"]


def test_writes_of_malformed_assignment_id_are_422(
    client: TestClient, key_headers: dict[str, str]
) -> None:
    """An id that cannot be a database id is an invalid input, not "not found"."""
    # the message comes from the database driver
    response = client.delete("/assignments/not-an-id", headers=key_headers)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert any("not-an-id" in message for message in response.json()["detail"])


def test_writes_of_fallback_assignment_are_422(
    client: TestClient, key_headers: dict[str, str]
) -> None:
    """The assignments of the fallback project can be neither removed nor updated over REST."""
    before = _assignments(client, FALLBACK_PROJECT_ID)
    assignment_id = before[0]["id"]

    for response in (
        client.delete(f"/assignments/{assignment_id}", headers=key_headers),
        client.post(
            f"/assignments/{assignment_id}/update_to_latest", headers=key_headers
        ),
    ):
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
        assert any(
            FALLBACK_PROJECT_ID in message for message in response.json()["detail"]
        )
    assert _assignments(client, FALLBACK_PROJECT_ID) == before


def test_writes_of_assignment_without_key_are_401(
    client: TestClient, assignment: dict[str, Any]
) -> None:
    """Without the API key the assignment stays as it is."""
    for response in (
        client.delete(f"/assignments/{assignment['id']}"),
        client.post(f"/assignments/{assignment['id']}/update_to_latest"),
    ):
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    assert _assignments(client, assignment["project_id"]) == [assignment]
