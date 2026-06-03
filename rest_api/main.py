"""FastAPI application for the AlphaKraken REST API."""

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from rest_api.routes import router
from shared.db.engine import connect_db

REST_API_PORT = int(os.getenv("REST_API_PORT", "8090"))


@asynccontextmanager
async def _lifespan(_app: FastAPI) -> AsyncIterator[None]:
    """Connect to the database on startup."""
    connect_db()
    yield


app = FastAPI(
    title="AlphaKraken REST API",
    description="Read-only API for querying raw files and metrics.",
    version="0.1.0",
    lifespan=_lifespan,
)

app.include_router(router)


@app.get("/health")
def _health() -> dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=REST_API_PORT)  # noqa: S104
