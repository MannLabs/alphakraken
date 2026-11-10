"""User queries on MongoDB."""

from shared.db import models
from shared.db._mongo.connection import ensure_connected
from shared.db._mongo.errors import translate_errors
from shared.db._mongo.mappers import to_user
from shared.domain.users import User, UserStatus


@translate_errors
def find(*, include_inactive: bool) -> list[User]:
    """Return the users, cf. `shared.db.users`."""
    ensure_connected()
    query = (
        models.User.objects
        if include_inactive
        else models.User.objects(status=UserStatus.ACTIVE)
    )
    return [to_user(doc) for doc in query.order_by("initials")]


@translate_errors
def project_owner(raw_file_id: str) -> User | None:
    """Return the active owner of the project of a raw file, cf. `shared.db.users`."""
    ensure_connected()
    raw_file = models.RawFile.objects(id=raw_file_id).only("project_id").first()
    if raw_file is None or not raw_file.has_project:
        return None
    project = models.Project.objects(id=raw_file.project_id).only("owner").first()
    owner = project.owner if project is not None else None
    if owner is None or owner.status != UserStatus.ACTIVE:
        return None
    return to_user(owner)
