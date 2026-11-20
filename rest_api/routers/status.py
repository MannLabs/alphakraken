"""Status endpoints."""

from fastapi import APIRouter

from rest_api.schemas import StatusResponse
from shared.db import status as status_store

router = APIRouter(prefix="/status")


@router.get("/")
def list_status() -> list[StatusResponse]:
    """List the status of all entities (instruments, file systems, jobs)."""
    return [StatusResponse.model_validate(s) for s in status_store.find()]
