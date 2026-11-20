"""Status endpoints."""

from fastapi import APIRouter

from rest_api.schemas import Page, StatusResponse
from shared.db import status as status_store

router = APIRouter(prefix="/status")


@router.get("/")
def list_status() -> Page[StatusResponse]:
    """List the status of all entities (instruments, file systems, jobs), on one page."""
    return Page.of_all([StatusResponse.of(s) for s in status_store.find()])
