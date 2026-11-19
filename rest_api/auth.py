"""API key check of the REST API."""

import os
import secrets
from typing import Annotated

from fastapi import HTTPException, Security, status
from fastapi.security import APIKeyHeader

API_KEY_ENV_VAR = "REST_API_KEY"
API_KEY_HEADER = "X-API-Key"

_api_key_header = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)


def require_api_key(
    api_key: Annotated[str | None, Security(_api_key_header)] = None,
) -> None:
    """Reject a request without the API key.

    Raises:
        HTTPException: 401 if the key is missing or wrong, or if no key is configured.

    """
    # read per request, so that an unset key fails closed instead of at startup
    expected = os.environ.get(API_KEY_ENV_VAR)
    if (
        not expected
        or api_key is None
        or not secrets.compare_digest(api_key.encode(), expected.encode())
    ):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or missing API key")
