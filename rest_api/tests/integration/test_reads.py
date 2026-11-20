"""Integration tests of the read endpoints.

They rely only on what seed.py guarantees, and find the data they need in the responses, as
the other tests add to the database.
"""

from datetime import datetime, timedelta

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from shared.db import raw_files as raw_file_store
from shared.db import status as status_store
from shared.db import users as user_store
from shared.domain.users import UserStatus
from shared.keys import FALLBACK_PROJECT_ID

MAX_LIMIT = 1000
METRICS_TYPES_IN_SEED = {"msqc", "alphadia", "diann"}


def _all(client: TestClient, path: str, **params: str | int | bool) -> dict:
    response = client.get(path, params={"limit": MAX_LIMIT, **params})
    assert response.status_code == status.HTTP_200_OK, response.text
    return response.json()


def test_health(client: TestClient) -> None:
    """The health endpoint answers."""
    assert client.get("/health").json() == {"status": "ok"}


@pytest.mark.parametrize("path", ["/raw_files/", "/projects/", "/settings/"])
@pytest.mark.parametrize("limit", [0, MAX_LIMIT + 1])
def test_list_rejects_limit_out_of_range(
    client: TestClient, path: str, limit: int
) -> None:
    """A limit outside 1..MAX_LIMIT is rejected."""
    assert (
        client.get(path, params={"limit": limit}).status_code
        == status.HTTP_422_UNPROCESSABLE_CONTENT
    )


@pytest.mark.parametrize(
    ("path", "params", "schema_name"),
    [
        ("/raw_files/", {}, "RawFileResponse"),
        ("/raw_files/", {"include_metrics": False}, "RawFileResponse"),
        ("/raw_files/", {"include_file_info": True}, "RawFileResponse"),
        ("/projects/", {}, "ProjectResponse"),
        ("/settings/", {}, "SettingsResponse"),
    ],
)
def test_responses_carry_exactly_the_declared_fields(
    client: TestClient, path: str, params: dict[str, bool], schema_name: str
) -> None:
    """Every entry has all fields of its OpenAPI schema, null when unset, and no others."""
    schemas = client.get("/openapi.json").json()["components"]["schemas"]
    declared = set(schemas[schema_name]["properties"])

    entries = _all(client, path, **params)["data"]

    assert entries
    assert all(set(entry) == declared for entry in entries)


def test_assignments_carry_exactly_the_declared_fields(client: TestClient) -> None:
    """Every assignment of a project has all fields of its OpenAPI schema and no others."""
    schemas = client.get("/openapi.json").json()["components"]["schemas"]
    declared = set(schemas["SettingsAssignmentResponse"]["properties"])

    assignments = [a for p in _all(client, "/projects/")["data"] for a in p["settings"]]

    assert assignments
    assert all(set(assignment) == declared for assignment in assignments)


# projects


def test_projects_lists_active_and_archived_with_assignments(
    client: TestClient,
) -> None:
    """Archived projects are listed with the active ones, each with its assignments."""
    body = _all(client, "/projects/")

    assert body["total"] == len(body["data"])
    assert {p["status"] for p in body["data"]} >= {"active", "inactive"}
    assignment = next(a for p in body["data"] for a in p["settings"])
    assert set(assignment) >= {
        "settings_id",
        "name",
        "version",
        "scopes",
        "excluded_scopes",
        "raw_file_id_filter",
        "raw_file_id_exclude_filter",
    }
    assert assignment["settings_id"] == f"{assignment['name']}_v{assignment['version']}"


def test_projects_pagination(client: TestClient) -> None:
    """Pages of projects do not overlap and report the same total."""
    first = client.get("/projects/", params={"limit": 1, "offset": 0}).json()
    second = client.get("/projects/", params={"limit": 1, "offset": 1}).json()

    assert first["total"] == second["total"] >= 2
    assert first["data"][0]["id"] != second["data"][0]["id"]


def test_project_by_id(client: TestClient) -> None:
    """A project is returned by its id, with its assignments."""
    project = client.get(f"/projects/{FALLBACK_PROJECT_ID}").json()

    assert project["id"] == FALLBACK_PROJECT_ID
    assert project["settings"]


def test_project_filter_by_id(client: TestClient) -> None:
    """The project id filter returns only that project."""
    body = _all(client, "/projects/", project_id=FALLBACK_PROJECT_ID)

    assert [p["id"] for p in body["data"]] == [FALLBACK_PROJECT_ID]


def test_unknown_project_is_404(client: TestClient) -> None:
    """An unknown project id is 404, naming the id."""
    response = client.get("/projects/NOSUCHPROJECT")

    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert "NOSUCHPROJECT" in response.json()["detail"]


# settings


def test_settings_lists_active_and_archived(client: TestClient) -> None:
    """Archived settings are listed with the active ones, each with its `<name>_v<version>` id."""
    body = _all(client, "/settings/")

    assert body["total"] == len(body["data"])
    assert {s["status"] for s in body["data"]} >= {"active", "inactive"}
    assert all(s["id"] == f"{s['name']}_v{s['version']}" for s in body["data"])


def test_settings_filter_by_name_returns_all_versions(client: TestClient) -> None:
    """The name filter returns all versions of the name, archived ones included."""
    archived = next(
        s for s in _all(client, "/settings/")["data"] if s["status"] == "inactive"
    )

    versions = _all(client, "/settings/", name=archived["name"])["data"]

    assert {s["name"] for s in versions} == {archived["name"]}
    assert archived["version"] in {s["version"] for s in versions}


def test_settings_by_id(client: TestClient) -> None:
    """Settings are returned by id as they appear in the list."""
    any_settings = _all(client, "/settings/")["data"][0]

    assert client.get(f"/settings/{any_settings['id']}").json() == any_settings


@pytest.mark.parametrize("settings_id", ["nosuch_v1", "no_version_suffix"])
def test_unknown_settings_is_404(client: TestClient, settings_id: str) -> None:
    """Unknown settings, also ids without version suffix, are 404."""
    assert (
        client.get(f"/settings/{settings_id}").status_code == status.HTTP_404_NOT_FOUND
    )


# raw files


def test_raw_files_total_matches_database(client: TestClient) -> None:
    """The total of raw files equals the count in the database."""
    assert _all(client, "/raw_files/")["total"] == raw_file_store.count(
        raw_file_store.RawFileFilter()
    )


def test_raw_files_carry_latest_metrics_of_each_type(client: TestClient) -> None:
    """Each raw file carries at most one metrics entry per type, with its output path."""
    raw_files = _all(client, "/raw_files/")["data"]

    metrics = [m for r in raw_files for m in r["metrics"]]
    assert {m["type"] for m in metrics} >= METRICS_TYPES_IN_SEED
    assert all("output_path" in m for m in metrics)
    for raw_file in raw_files:
        types = [m["type"] for m in raw_file["metrics"]]
        assert len(types) == len(set(types)), raw_file["id"]


def test_raw_files_without_metrics(client: TestClient) -> None:
    """Without metrics requested, no raw file carries metrics."""
    raw_files = _all(client, "/raw_files/", include_metrics=False)["data"]

    assert all(r["metrics"] is None for r in raw_files)


def test_raw_files_file_info_only_on_request(client: TestClient) -> None:
    """File info and backup path are returned only when requested."""
    without = _all(client, "/raw_files/")["data"]
    with_info = _all(client, "/raw_files/", include_file_info=True)["data"]

    assert all(r["file_info"] is None and r["backup_path"] is None for r in without)
    assert all(r["backup_path"] for r in with_info)
    assert any(r.get("file_info") for r in with_info)


def test_raw_files_filter_by_instrument(client: TestClient) -> None:
    """The instrument filter returns only files of that instrument."""
    instrument_id = _all(client, "/raw_files/")["data"][0]["instrument_id"]

    found = _all(client, "/raw_files/", instrument_id=instrument_id)["data"]

    assert found
    assert {r["instrument_id"] for r in found} == {instrument_id}


def test_raw_files_filter_by_project(client: TestClient) -> None:
    """The project filter returns only files of that project."""
    found = _all(client, "/raw_files/", project_id=FALLBACK_PROJECT_ID)["data"]

    assert found
    assert {r["project_id"] for r in found} == {FALLBACK_PROJECT_ID}


def test_raw_files_name_contains_is_case_insensitive(client: TestClient) -> None:
    """The name filter matches substrings regardless of case."""
    raw_file_id = _all(client, "/raw_files/")["data"][0]["id"]
    part = raw_file_id[2:8]

    found = _all(client, "/raw_files/", name_contains=part.swapcase())["data"]

    assert raw_file_id in {r["id"] for r in found}
    assert all(part.lower() in r["id"].lower() for r in found)


def test_raw_files_filter_by_creation_date(client: TestClient) -> None:
    """The date filters split the files at a creation time, bounds included."""
    raw_files = _all(client, "/raw_files/")["data"]
    oldest = min(datetime.fromisoformat(r["created_at"]) for r in raw_files)
    one_ms = timedelta(milliseconds=1)

    after = _all(client, "/raw_files/", date_from=(oldest + one_ms).isoformat())["data"]
    until = _all(client, "/raw_files/", date_to=oldest.isoformat())["data"]

    assert len(after) + len(until) == len(raw_files)
    assert all(datetime.fromisoformat(r["created_at"]) == oldest for r in until)


def test_raw_files_pagination(client: TestClient) -> None:
    """Pages of raw files do not overlap and report the same total."""
    first = client.get("/raw_files/", params={"limit": 2, "offset": 0}).json()
    second = client.get("/raw_files/", params={"limit": 2, "offset": 2}).json()

    assert first["total"] == second["total"]
    assert not {r["id"] for r in first["data"]} & {r["id"] for r in second["data"]}


# status and users


def test_status_lists_all_entities(client: TestClient, unique_id: str) -> None:
    """A status written to the database is returned, without a key."""
    status_store.update(
        unique_id, status="ok", status_details="set by a test", free_space_gb=42
    )

    response = client.get("/status/")

    assert response.status_code == status.HTTP_200_OK
    page = response.json()
    assert (page["total"], page["offset"], page["limit"]) == (
        len(page["data"]),
        0,
        len(page["data"]),
    )
    entry = next(s for s in page["data"] if s["id"] == unique_id)
    assert entry["status"] == "ok"
    assert entry["status_details"] == "set by a test"
    assert entry["free_space_gb"] == 42
    assert entry["entity_type"] == "instrument"
    assert set(entry) == {
        "id",
        "status",
        "status_details",
        "free_space_gb",
        "entity_type",
        "updated_at_",
    }


def test_users_lists_active_users_only(
    client: TestClient, key_headers: dict[str, str], unique_id: str
) -> None:
    """Active users are returned with email and initials only; inactive ones are left out."""
    # initials are letters only
    letters = "".join(chr(ord("a") + int(c, 16)) for c in unique_id[-6:])
    active = user_store.add(email=f"{letters}@example.org", initials=letters)
    inactive = user_store.add(email=f"x{letters}@example.org", initials=f"x{letters}")
    user_store.set_status(inactive.email, UserStatus.INACTIVE)

    response = client.get("/users/", headers=key_headers)

    assert response.status_code == status.HTTP_200_OK
    users = response.json()["data"]
    assert {"email": active.email, "initials": active.initials} in users
    assert inactive.email not in {u["email"] for u in users}


def test_users_without_key_is_401(client: TestClient) -> None:
    """The users, i.e. their emails, need the API key."""
    assert client.get("/users/").status_code == status.HTTP_401_UNAUTHORIZED
