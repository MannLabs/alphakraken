"""FastAPI application for the AlphaKraken REST API."""

import os

import uvicorn
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse

from rest_api.routes import router
from shared.domain.errors import NotFoundError

REST_API_PORT = int(os.getenv("REST_API_PORT", "8090"))

app = FastAPI(
    title="AlphaKraken REST API",
    description="Read-only API for querying raw files and metrics, projects and settings.",
    version="0.1.0",
)

app.include_router(router)


@app.exception_handler(NotFoundError)
def _not_found(_request: Request, error: NotFoundError) -> JSONResponse:
    return JSONResponse({"detail": str(error)}, status.HTTP_404_NOT_FOUND)


@app.get("/health")
def _health() -> dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=REST_API_PORT)  # noqa: S104
