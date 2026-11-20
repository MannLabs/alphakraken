"""Fixtures of the integration tests: the app in-process against a real MongoDB, cf. README.md."""

import os
import uuid
from collections.abc import Iterator
from unittest.mock import patch

import mongoengine
import pytest
from fastapi.testclient import TestClient
from pymongo.errors import PyMongoError

from rest_api.auth import API_KEY_ENV_VAR, API_KEY_HEADER
from rest_api.main import app
from rest_api.tests.integration.seed import seed
from shared.db._mongo import connection

MONGO_HOST = "localhost"
MONGO_PORT_ENV_VAR = "INTEGRATION_MONGO_PORT"
# not Mongo's default port, to never meet the database of a local AlphaKraken stack
DEFAULT_MONGO_PORT = 37018
DB_NAME = "krakendb_integration"
SERVER_SELECTION_TIMEOUT_MS = 2000
# set by GitHub Actions
CI_ENV_VAR = "CI"

API_KEY = "integration-key"


@pytest.fixture(scope="package", autouse=True)
def database() -> Iterator[None]:
    """A freshly seeded database that `shared.db` is connected to.

    Without a MongoDB the tests are skipped; in CI they fail.
    """
    port = int(os.environ.get(MONGO_PORT_ENV_VAR, DEFAULT_MONGO_PORT))
    mongoengine.disconnect()
    client = mongoengine.connect(
        DB_NAME,
        host=MONGO_HOST,
        port=port,
        serverSelectionTimeoutMS=SERVER_SELECTION_TIMEOUT_MS,
    )
    try:
        client.drop_database(DB_NAME)
    except PyMongoError:
        mongoengine.disconnect()
        message = f"No MongoDB at {MONGO_HOST}:{port}, cf. rest_api/tests/integration/README.md"
        if os.environ.get(CI_ENV_VAR):
            pytest.fail(message, pytrace=False)
        pytest.skip(message)

    # `shared.db` would connect with the credentials of a deployment
    with patch.object(connection, "connect_db"):
        seed()
        yield

    client.drop_database(DB_NAME)
    mongoengine.disconnect()
    connection.ensure_connected.cache_clear()


@pytest.fixture(scope="package")
def client() -> TestClient:
    """A client for the REST API."""
    return TestClient(app)


@pytest.fixture(autouse=True)
def api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    """Configure the API key."""
    monkeypatch.setenv(API_KEY_ENV_VAR, API_KEY)


@pytest.fixture(scope="package")
def key_headers() -> dict[str, str]:
    """The header with the API key that the writes need."""
    return {API_KEY_HEADER: API_KEY}


@pytest.fixture
def unique_id() -> str:
    """An id no other test uses, valid as a project id (upper-case letters and digits)."""
    return f"IT{uuid.uuid4().hex[:6].upper()}"


@pytest.fixture
def unique_settings_name(unique_id: str) -> str:
    """A settings name no other test uses."""
    return f"it_{unique_id.lower()}"
