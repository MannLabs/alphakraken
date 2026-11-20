"""Integration tests of the settings writes; the effects are checked over REST."""

from typing import Any

from fastapi import status
from fastapi.testclient import TestClient


def _new_settings(name: str, **fields: Any) -> dict[str, Any]:
    return {
        "name": name,
        "software_type": "msqc",
        "software": "msqc-1.0.0",
        "runner_name": "slurm",
        "metrics_type": "msqc",
        **fields,
    }


def _versions(client: TestClient, name: str) -> dict[int, str]:
    """The status of each version of the name."""
    response = client.get("/settings/", params={"name": name})
    return {s["version"]: s["status"] for s in response.json()["data"]}


def test_create_settings(
    client: TestClient, key_headers: dict[str, str], unique_settings_name: str
) -> None:
    """Created settings are version 1, with the resource defaults, and readable over REST."""
    response = client.post(
        "/settings/",
        json=_new_settings(unique_settings_name, description="created by a test"),
        headers=key_headers,
    )

    assert response.status_code == status.HTTP_201_CREATED, response.text
    created = response.json()
    assert created["id"] == f"{unique_settings_name}_v1"
    assert created["version"] == 1
    assert created["status"] == "active"
    assert created["description"] == "created by a test"
    assert created["slurm_cpus_per_task"] >= 1
    assert created["slurm_mem"]
    assert client.get(f"/settings/{created['id']}").json() == created


def test_create_settings_again_is_the_next_version(
    client: TestClient, key_headers: dict[str, str], unique_settings_name: str
) -> None:
    """A second create of a name gives version 2 and keeps version 1 active."""
    body = _new_settings(unique_settings_name)
    client.post("/settings/", json=body, headers=key_headers)

    response = client.post("/settings/", json=body, headers=key_headers)

    assert response.status_code == status.HTTP_201_CREATED, response.text
    assert response.json()["version"] == 2
    assert _versions(client, unique_settings_name) == {1: "active", 2: "active"}


def test_create_settings_archiving_the_previous_version(
    client: TestClient, key_headers: dict[str, str], unique_settings_name: str
) -> None:
    """`archive_previous` archives the latest active version."""
    body = _new_settings(unique_settings_name)
    client.post("/settings/", json=body, headers=key_headers)

    response = client.post(
        "/settings/",
        params={"archive_previous": True},
        json=body,
        headers=key_headers,
    )

    assert response.status_code == status.HTTP_201_CREATED, response.text
    assert _versions(client, unique_settings_name) == {1: "inactive", 2: "active"}


def test_create_settings_with_unknown_runner_is_422(
    client: TestClient, key_headers: dict[str, str], unique_settings_name: str
) -> None:
    """A runner that is not in the yaml is an invalid input, and nothing is written."""
    response = client.post(
        "/settings/",
        json=_new_settings(unique_settings_name, runner_name="nosuchrunner"),
        headers=key_headers,
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert any("nosuchrunner" in message for message in response.json()["detail"])
    assert _versions(client, unique_settings_name) == {}


def test_create_settings_with_invalid_body_is_422(
    client: TestClient, key_headers: dict[str, str], unique_settings_name: str
) -> None:
    """All violations of a body are reported at once, without a `body.` prefix."""
    response = client.post(
        "/settings/",
        json=_new_settings(
            unique_settings_name,
            software_type="nosuchsoftware",
            slurm_time="1h",
            fasta_file="typo",
        ),
        headers=key_headers,
    )

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    fields = {message.split(":")[0] for message in response.json()["detail"]}
    assert fields >= {"software_type", "slurm_time", "fasta_file"}
    assert _versions(client, unique_settings_name) == {}


def test_create_settings_without_key_is_401(
    client: TestClient, unique_settings_name: str
) -> None:
    """Without the API key nothing is written."""
    response = client.post("/settings/", json=_new_settings(unique_settings_name))

    assert response.status_code == status.HTTP_401_UNAUTHORIZED
    assert _versions(client, unique_settings_name) == {}


def _created(client: TestClient, key_headers: dict[str, str], name: str) -> str:
    """Create settings and return their id."""
    response = client.post("/settings/", json=_new_settings(name), headers=key_headers)
    assert response.status_code == status.HTTP_201_CREATED, response.text
    return response.json()["id"]


def test_archive_settings(
    client: TestClient, key_headers: dict[str, str], unique_settings_name: str
) -> None:
    """Archived settings are inactive; archiving them again is an invalid input."""
    settings_id = _created(client, key_headers, unique_settings_name)

    response = client.post(f"/settings/{settings_id}/archive", headers=key_headers)

    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
    assert client.get(f"/settings/{settings_id}").json()["status"] == "inactive"
    again = client.post(f"/settings/{settings_id}/archive", headers=key_headers)
    assert again.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert isinstance(again.json()["detail"], list)


def test_delete_settings(
    client: TestClient, key_headers: dict[str, str], unique_settings_name: str
) -> None:
    """Deleted settings are no longer returned; for further writes they are not found."""
    settings_id = _created(client, key_headers, unique_settings_name)

    response = client.delete(f"/settings/{settings_id}", headers=key_headers)

    assert response.status_code == status.HTTP_204_NO_CONTENT, response.text
    assert (
        client.get(f"/settings/{settings_id}").status_code == status.HTTP_404_NOT_FOUND
    )
    assert _versions(client, unique_settings_name) == {}
    for again in (
        client.delete(f"/settings/{settings_id}", headers=key_headers),
        client.post(f"/settings/{settings_id}/archive", headers=key_headers),
    ):
        assert again.status_code == status.HTTP_404_NOT_FOUND
        assert again.json() == {"detail": f"No settings {settings_id}"}


def test_delete_assigned_settings_is_422(
    client: TestClient, key_headers: dict[str, str]
) -> None:
    """Settings assigned to an active project cannot be deleted; the error names the project."""
    project, assignment = next(
        (p, a)
        for p in client.get("/projects/", params={"limit": 1000}).json()["data"]
        if p["status"] == "active"
        for a in p["settings"]
        if client.get(f"/settings/{a['settings_id']}").json()["status"] == "active"
    )
    settings_id = assignment["settings_id"]

    response = client.delete(f"/settings/{settings_id}", headers=key_headers)

    assert response.status_code == status.HTTP_422_UNPROCESSABLE_CONTENT
    assert any(project["id"] in message for message in response.json()["detail"])
    assert client.get(f"/settings/{settings_id}").json()["status"] == "active"


def test_archive_and_delete_unknown_settings_is_404(
    client: TestClient, key_headers: dict[str, str]
) -> None:
    """Unknown settings in the path are not found."""
    for response in (
        client.post("/settings/nosuch_v1/archive", headers=key_headers),
        client.delete("/settings/nosuch_v1", headers=key_headers),
    ):
        assert response.status_code == status.HTTP_404_NOT_FOUND
        assert "nosuch_v1" in response.json()["detail"]


def test_archive_and_delete_settings_without_key_is_401(
    client: TestClient, key_headers: dict[str, str], unique_settings_name: str
) -> None:
    """Without the API key the settings stay active."""
    settings_id = _created(client, key_headers, unique_settings_name)

    for response in (
        client.post(f"/settings/{settings_id}/archive"),
        client.delete(f"/settings/{settings_id}"),
    ):
        assert response.status_code == status.HTTP_401_UNAUTHORIZED
    assert client.get(f"/settings/{settings_id}").json()["status"] == "active"
