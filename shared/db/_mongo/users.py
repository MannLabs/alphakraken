"""User queries on MongoDB."""

from shared.db._mongo import models
from shared.db._mongo.connection import db_backend_function
from shared.db._mongo.mappers import to_user
from shared.domain.errors import NotFoundError
from shared.domain.users import User, UserStatus


@db_backend_function
def find(*, include_inactive: bool) -> list[User]:
    """Return the users, cf. `shared.db.users`."""
    query = (
        models.User.objects
        if include_inactive
        else models.User.objects(status=UserStatus.ACTIVE)
    )
    return [to_user(doc) for doc in query.order_by("initials")]


@db_backend_function
def project_owner(raw_file_id: str) -> User | None:
    """Return the active owner of the project of a raw file, cf. `shared.db.users`."""
    raw_file = models.RawFile.objects(id=raw_file_id).only("project_id").first()
    if raw_file is None or not raw_file.has_project:
        return None
    project = models.Project.objects(id=raw_file.project_id).only("owner").first()
    owner = project.owner if project is not None else None
    if owner is None or owner.status != UserStatus.ACTIVE:
        return None
    return to_user(owner)


@db_backend_function
def add(*, email: str, initials: str, messenger_id: str | None) -> User:
    """Add a new active user, cf. `shared.db.users`."""
    user = models.User(email=email, initials=initials, messenger_id=messenger_id)
    user.save(force_insert=True)
    return to_user(user)


@db_backend_function
def update(email: str, *, initials: str, messenger_id: str | None) -> None:
    """Update the initials and messenger id of a user, cf. `shared.db.users`."""
    user = models.User.objects.get(email=email)
    user.initials = initials
    user.messenger_id = messenger_id
    # save() instead of update() to run the field validation
    user.save()


@db_backend_function
def set_status(email: str, status: str) -> None:
    """Set the status of a user, cf. `shared.db.users`."""
    if not models.User.objects(email=email).update_one(status=status):
        raise NotFoundError(f"No user {email}")
