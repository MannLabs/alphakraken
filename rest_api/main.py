"""FastAPI application for the AlphaKraken REST API."""

import os

import uvicorn
from fastapi import FastAPI

from rest_api.routes import router

REST_API_PORT = int(os.getenv("REST_API_PORT", "8090"))

app = FastAPI(
    title="AlphaKraken REST API",
    description="Read-only API for querying raw files and metrics, projects and settings.",
    version="0.1.0",
)

app.include_router(router)


@app.get("/health")
def _health() -> dict[str, str]:
    return {"status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=REST_API_PORT)  # noqa: S104
