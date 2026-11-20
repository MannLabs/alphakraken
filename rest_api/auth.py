"""API key check of the REST API."""

import os
import secrets
from typing import Annotated

from fastapi import HTTPException, Security, status
from fastapi.responses import JSONResponse
from fastapi.security import APIKeyHeader
from starlette.datastructures import Headers
from starlette.types import ASGIApp, Receive, Scope, Send

API_KEY_ENV_VAR = "REST_API_KEY"
API_KEY_HEADER = "X-API-Key"
INVALID_API_KEY_MESSAGE = "Invalid or missing API key"

# methods that do not change anything, so their routes decide themselves whether they need the key
_SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})

_api_key_header = APIKeyHeader(name=API_KEY_HEADER, auto_error=False)


def _is_valid(api_key: str | None) -> bool:
    """Return whether the key is the configured one; never if none is configured."""
    # read per request: the reads must work without a configured key
    expected = os.environ.get(API_KEY_ENV_VAR)
    return bool(
        expected
        and api_key is not None
        and secrets.compare_digest(api_key.encode(), expected.encode())
    )


def require_api_key(
    api_key: Annotated[str | None, Security(_api_key_header)] = None,
) -> None:
    """Reject a request without the API key.

    Raises:
        HTTPException: 401 if the key is missing or wrong, or if no key is configured.

    """
    if not _is_valid(api_key):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, INVALID_API_KEY_MESSAGE)


class RejectWritesWithoutApiKey:
    """ASGI middleware answering every write without the API key with 401, before its body is read.

    A route dependency runs only after FastAPI has read and parsed the body, so on its own
    `require_api_key` would let an unkeyed request upload a body and get a 422 for it.
    """

    def __init__(self, app: ASGIApp) -> None:
        """Wrap the application."""
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        """Pass on the request, or answer 401 if it is a write without the key."""
        if (
            scope["type"] == "http"
            and scope["method"] not in _SAFE_METHODS
            and not _is_valid(Headers(scope=scope).get(API_KEY_HEADER))
        ):
            response = JSONResponse(
                {"detail": INVALID_API_KEY_MESSAGE}, status.HTTP_401_UNAUTHORIZED
            )
            await response(scope, receive, send)
            return
        await self.app(scope, receive, send)
