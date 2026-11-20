"""User endpoints."""

from fastapi import APIRouter, Depends

from rest_api.auth import require_api_key
from rest_api.schemas import UNAUTHORIZED, Page, UserResponse
from shared.db import users as user_store

router = APIRouter(prefix="/users")


@router.get("/", dependencies=[Depends(require_api_key)], responses=UNAUTHORIZED)
def list_users() -> Page[UserResponse]:
    """List the active users, i.e. the possible owners of projects and settings, on one page."""
    return Page.of_all([UserResponse.model_validate(u) for u in user_store.find()])
