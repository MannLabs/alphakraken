"""FastAPI application for the AlphaKraken REST API."""

import os
from typing import Any

import uvicorn
from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from rest_api.routers import projects, raw_files, settings
from shared.domain.errors import AlreadyExistsError, InvalidInputError, NotFoundError
from shared.domain.inputs import message

# first element of the location of an error in the request body
_BODY_LOCATION = "body"

REST_API_PORT = int(os.getenv("REST_API_PORT", "8090"))

app = FastAPI(
    title="AlphaKraken REST API",
    description="Read-only API for querying raw files and metrics, projects and settings.",
    version="0.1.0",
)

app.include_router(raw_files.router)
app.include_router(projects.router)
app.include_router(settings.router)


@app.exception_handler(NotFoundError)
def _not_found(_request: Request, error: NotFoundError) -> JSONResponse:
    return JSONResponse({"detail": str(error)}, status.HTTP_404_NOT_FOUND)


@app.exception_handler(AlreadyExistsError)
def _already_exists(_request: Request, error: AlreadyExistsError) -> JSONResponse:
    return JSONResponse({"detail": str(error)}, status.HTTP_409_CONFLICT)


@app.exception_handler(InvalidInputError)
def _invalid_input(_request: Request, error: InvalidInputError) -> JSONResponse:
    return JSONResponse({"detail": error.errors}, status.HTTP_422_UNPROCESSABLE_CONTENT)


@app.exception_handler(RequestValidationError)
def _invalid_request(_request: Request, error: RequestValidationError) -> JSONResponse:
    """Report the errors of FastAPI's validation in the format of `InvalidInputError`."""
    return JSONResponse(
        {"detail": [_request_error_message(e) for e in error.errors()]},
        status.HTTP_422_UNPROCESSABLE_CONTENT,
    )


def _request_error_message(error: dict[str, Any]) -> str:
    location = error["loc"]
    # a body error names its field like the domain errors do: `name: ...`
    if location and location[0] == _BODY_LOCATION:
        location = location[1:]
    return message({**error, "loc": location})


@app.get("/health")
def _health() -> dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=REST_API_PORT)  # noqa: S104
