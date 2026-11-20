"""Tests for the API key check, on every route that needs the key."""

import re

import pytest
from fastapi import status
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from rest_api.auth import API_KEY_ENV_VAR, API_KEY_HEADER, require_api_key
from rest_api.main import app

client = TestClient(app)

API_KEY = "some-key"
READ_METHODS = {"GET", "HEAD", "OPTIONS"}
# the GET routes that return personal data
KEYED_READS = {("GET", "/users/")}


def _declares_key(route: APIRoute) -> bool:
    return any(d.call is require_api_key for d in route.dependant.dependencies)


ROUTES = [route for route in app.routes if isinstance(route, APIRoute)]
WRITES = sorted(
    (method, route.path) for route in ROUTES for method in route.methods - READ_METHODS
)
KEYED = sorted(
    (method, route.path)
    for route in ROUTES
    if _declares_key(route)
    for method in route.methods
)


def _url(path: str) -> str:
    """Fill the path parameters with a placeholder."""
    return re.sub(r"{[^}]+}", "some_id", path)


def test_every_write_declares_the_key() -> None:
    """All writes, and of the reads only the users, need the key."""
    assert WRITES
    assert set(KEYED) == set(WRITES) | KEYED_READS


@pytest.mark.parametrize(
    ("configured_key", "headers"),
    [
        (API_KEY, {}),
        (API_KEY, {API_KEY_HEADER: "wrong-key"}),
        (API_KEY, {API_KEY_HEADER: ""}),
        # without a configured key no key is right, also not an empty one
        (None, {API_KEY_HEADER: API_KEY}),
        (None, {API_KEY_HEADER: ""}),
        (None, {}),
        ("", {API_KEY_HEADER: ""}),
    ],
)
@pytest.mark.parametrize(("method", "path"), KEYED)
def test_keyed_route_without_valid_key_is_401(
    monkeypatch: pytest.MonkeyPatch,
    method: str,
    path: str,
    configured_key: str | None,
    headers: dict[str, str],
) -> None:
    """A missing or wrong key, or no configured key, gives 401 on every keyed route."""
    # the database is not patched: a request that got past the check would fail differently
    if configured_key is None:
        monkeypatch.delenv(API_KEY_ENV_VAR, raising=False)
    else:
        monkeypatch.setenv(API_KEY_ENV_VAR, configured_key)

    response = client.request(method, _url(path), headers=headers)

    assert response.status_code == status.HTTP_401_UNAUTHORIZED
    assert response.json() == {"detail": "Invalid or missing API key"}
