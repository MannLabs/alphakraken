"""Tests for the response models of the REST API."""

from dataclasses import fields

import pytest
from fastapi.testclient import TestClient
from pydantic import BaseModel

from rest_api.main import app
from rest_api.schemas import (
    ErrorResponse,
    InvalidInputResponse,
    ProjectResponse,
    RawFileResponse,
    SettingsAssignmentResponse,
    SettingsResponse,
    StatusResponse,
    UserResponse,
)
from shared.domain.projects import Project, ProjectSettings
from shared.domain.raw_files import RawFile
from shared.domain.settings import Settings
from shared.domain.status import KrakenStatus
from shared.domain.users import User
from shared.tests.factories import settings


def test_settings_response_of_legacy_settings() -> None:
    """Legacy settings, with values the domain type does not allow, can be served."""
    # the type hints of the domain do not hold for documents written before the validation
    legacy = settings(number=-1, runner_name=None, metrics_type=None, owner_email=None)

    response = SettingsResponse.model_validate(legacy)

    assert response.number == -1
    assert response.runner_name is None
    assert response.metrics_type is None


@pytest.mark.parametrize(
    ("domain", "not_returned", "response", "joined"),
    [
        (RawFile, {"created_at_"}, RawFileResponse, {"metrics", "backup_path"}),
        (Project, set(), ProjectResponse, {"settings"}),
        (Settings, set(), SettingsResponse, set()),
        (
            ProjectSettings,
            # the settings are returned as their id, name and version
            {"settings", "created_at_"},
            SettingsAssignmentResponse,
            {"settings_id", "name", "version"},
        ),
        (KrakenStatus, set(), StatusResponse, set()),
        (User, {"messenger_id", "status", "created_at_"}, UserResponse, set()),
    ],
)
def test_response_returns_every_domain_field(
    domain: type, not_returned: set[str], response: type[BaseModel], joined: set[str]
) -> None:
    """A field added to a domain type must be added to its response, or be left out here on purpose."""
    domain_fields = {field.name for field in fields(domain)}

    assert domain_fields - not_returned == set(response.model_fields) - joined


def test_openapi_lists_all_raw_file_fields() -> None:
    """The schema of a raw file lists the fields of the model and allows no undeclared ones."""
    schema = app.openapi()["components"]["schemas"]["RawFileResponse"]

    assert set(schema["properties"]) == set(RawFileResponse.model_fields)
    assert schema.get("additionalProperties") is not True


WRITE_METHODS = {"post", "put", "patch", "delete"}
ERROR_SCHEMA = {"$ref": "#/components/schemas/ErrorResponse"}
INVALID_INPUT_SCHEMA = {"$ref": "#/components/schemas/InvalidInputResponse"}


def _schema(operation: dict, status_code: str) -> dict:
    return operation["responses"][status_code]["content"]["application/json"]["schema"]


def test_openapi_documents_the_errors_as_they_are_returned() -> None:
    """No route documents FastAPI's default validation error; writes document 401 and 422."""
    openapi = app.openapi()

    assert "HTTPValidationError" not in openapi["components"]["schemas"]
    writes = [
        (path, method, operation)
        for path, operations in openapi["paths"].items()
        for method, operation in operations.items()
        if method in WRITE_METHODS
    ]
    assert writes
    for path, method, operation in writes:
        assert _schema(operation, "401") == ERROR_SCHEMA, (method, path)
        assert _schema(operation, "422") == INVALID_INPUT_SCHEMA, (method, path)
        if "{" in path:
            assert _schema(operation, "404") == ERROR_SCHEMA, (method, path)


def test_openapi_documents_the_remaining_errors() -> None:
    """Conflicts on create, the key of the users, and unknown ids of the reads are documented."""
    paths = app.openapi()["paths"]

    assert _schema(paths["/projects/"]["post"], "409") == ERROR_SCHEMA
    assert _schema(paths["/settings/"]["post"], "409") == ERROR_SCHEMA
    assert _schema(paths["/users/"]["get"], "401") == ERROR_SCHEMA
    assert _schema(paths["/projects/{project_id}"]["get"], "404") == ERROR_SCHEMA
    assert _schema(paths["/settings/{settings_id}"]["get"], "404") == ERROR_SCHEMA
    assert _schema(paths["/projects/"]["get"], "422") == INVALID_INPUT_SCHEMA


def test_error_responses_match_their_models() -> None:
    """The bodies the handlers return validate against the documented models."""
    client = TestClient(app)

    ErrorResponse.model_validate(client.post("/projects/", json={}).json())
    InvalidInputResponse.model_validate(
        client.get("/projects/", params={"limit": 0}).json()
    )
