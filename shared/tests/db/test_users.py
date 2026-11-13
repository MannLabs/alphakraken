"""Tests for the users API and its Mongo backend."""

from collections.abc import Iterator
from datetime import UTC, datetime
from unittest.mock import MagicMock, call, patch

import pytest

from shared.db import users
from shared.db._mongo import models
from shared.db._mongo import users as backend
from shared.domain.errors import NotFoundError, ValidationError
from shared.domain.users import NewUser, UserStatus, UserUpdate
from shared.keys import FALLBACK_PROJECT_ID
from shared.tests.factories import user

NAIVE_TIME = datetime(2024, 1, 2, 3, 4, 5)  # noqa: DTZ001 as stored by pymongo
AWARE_TIME = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)


def _user_document(status: str = UserStatus.ACTIVE) -> models.User:
    return models.User(
        email="masc@example.org",
        initials="MaSc",
        messenger_id="U_MASC",
        status=status,
        created_at_=NAIVE_TIME,
    )


@pytest.fixture(autouse=True)
def _connected() -> Iterator[None]:
    with patch("shared.db._mongo.connection.ensure_connected"):
        yield


@patch("shared.db._mongo.users.models")
def test_find_active_users(mock_models: MagicMock) -> None:
    """By default only active users are returned, ordered by initials."""
    mock_models.User.objects.return_value.order_by.return_value = [_user_document()]

    assert users.find() == [user(created_at_=AWARE_TIME)]
    mock_models.User.objects.assert_called_once_with(status=UserStatus.ACTIVE)
    mock_models.User.objects.return_value.order_by.assert_called_once_with("initials")


@patch("shared.db._mongo.users.models")
def test_find_all_users(mock_models: MagicMock) -> None:
    """Inactive users are included on request."""
    mock_models.User.objects.order_by.return_value = [
        _user_document(UserStatus.INACTIVE)
    ]

    assert users.find(include_inactive=True) == [
        user(status=UserStatus.INACTIVE, created_at_=AWARE_TIME)
    ]
    mock_models.User.objects.assert_not_called()


def _mock_owner(
    mock_models: MagicMock, project_id: str | None, owner: models.User | None
) -> None:
    """Set up the raw file and project lookups; a project id of None means no raw file."""
    raw_file = (
        None
        if project_id is None
        else models.RawFile(id="f1", original_name="f1", project_id=project_id)
    )
    mock_models.RawFile.objects.return_value.only.return_value.first.return_value = (
        raw_file
    )
    mock_models.Project.objects.return_value.only.return_value.first.return_value = (
        MagicMock(owner=owner)
    )


@patch("shared.db._mongo.users.models")
def test_project_owner(mock_models: MagicMock) -> None:
    """The active owner of the raw file's project is returned."""
    _mock_owner(mock_models, "P1", _user_document())

    assert users.project_owner("f1") == user(created_at_=AWARE_TIME)
    mock_models.RawFile.objects.assert_called_once_with(id="f1")
    mock_models.Project.objects.assert_called_once_with(id="P1")


@pytest.mark.parametrize(
    ("project_id", "owner"),
    [
        (None, _user_document()),  # unknown raw file
        (FALLBACK_PROJECT_ID, _user_document()),  # no project
        ("P1", None),  # project without owner
        ("P1", _user_document(UserStatus.INACTIVE)),  # inactive owner
    ],
)
@patch("shared.db._mongo.users.models")
def test_project_owner_returns_none(
    mock_models: MagicMock, project_id: str | None, owner: models.User | None
) -> None:
    """Without a raw file, a project or an active owner there is no owner."""
    _mock_owner(mock_models, project_id, owner)

    assert users.project_owner("f1") is None


@patch("shared.db._mongo.users.models")
def test_backend_add(mock_models: MagicMock) -> None:
    """The backend inserts the user, never overwriting an existing one."""
    mock_models.User.return_value = _user_document()

    with patch.object(models.User, "save") as mock_save:
        result = backend.add(
            NewUser(email="masc@example.org", initials="MaSc", messenger_id="U_MASC")
        )

    mock_models.User.assert_called_once_with(
        email="masc@example.org", initials="MaSc", messenger_id="U_MASC"
    )
    mock_save.assert_called_once_with(force_insert=True)
    assert result == user(created_at_=AWARE_TIME)


@patch("shared.db._mongo.users.models")
def test_backend_update(mock_models: MagicMock) -> None:
    """The backend saves the document, to run its field validation."""
    backend.update("a@b.de", UserUpdate(initials="NB", messenger_id="U123"))

    mock_models.User.objects.get.assert_called_once_with(email="a@b.de")
    document = mock_models.User.objects.get.return_value
    assert document.initials == "NB"
    assert document.messenger_id == "U123"
    document.save.assert_called_once()


@pytest.mark.parametrize(("count", "taken"), [(0, False), (1, True)])
@patch("shared.db._mongo.users.models")
def test_backend_email_and_initials_taken(
    mock_models: MagicMock, count: int, *, taken: bool
) -> None:
    """Emails and initials are looked up among all users, initials optionally leaving one user out."""
    mock_models.User.objects.return_value.count.return_value = count

    assert backend.email_taken("a@b.de") is taken
    assert backend.initials_taken("AB") is taken
    assert backend.initials_taken("AB", except_email="a@b.de") is taken

    assert mock_models.User.objects.call_args_list == [
        call(email="a@b.de"),
        call(initials="AB", email__ne=None),
        call(initials="AB", email__ne="a@b.de"),
    ]


@pytest.mark.parametrize("status", [UserStatus.ACTIVE, UserStatus.INACTIVE])
@patch("shared.db._mongo.users.models")
def test_require_active(mock_models: MagicMock, status: str) -> None:
    """An active user passes, an inactive one is a validation error."""
    mock_models.User.objects.get.return_value = _user_document(status)

    if status == UserStatus.ACTIVE:
        users.require_active("masc@example.org")
    else:
        with pytest.raises(ValidationError, match="is inactive"):
            users.require_active("masc@example.org")

    mock_models.User.objects.get.assert_called_once_with(email="masc@example.org")


@pytest.fixture
def taken() -> Iterator[dict[str, MagicMock]]:
    """No email and no initials are taken; the backend writes are mocked."""
    with (
        patch("shared.db._mongo.users.email_taken", return_value=False) as email_taken,
        patch(
            "shared.db._mongo.users.initials_taken", return_value=False
        ) as initials_taken,
        patch("shared.db._mongo.users.add") as backend_add,
        patch("shared.db._mongo.users.update") as backend_update,
    ):
        yield {
            "email": email_taken,
            "initials": initials_taken,
            "backend_add": backend_add,
            "backend_update": backend_update,
        }


def test_add_cleans_fields(taken: dict[str, MagicMock]) -> None:
    """A user with unused email and initials is added with the fields cleaned."""
    result = users.add(email=" New@B.de ", initials=" NB ", messenger_id=" ")

    new = NewUser(email="new@b.de", initials="NB", messenger_id=None)
    taken["email"].assert_called_once_with("new@b.de")
    taken["initials"].assert_called_once_with("NB")
    taken["backend_add"].assert_called_once_with(new)
    assert result == taken["backend_add"].return_value


def test_add_rejects_invalid_fields(taken: dict[str, MagicMock]) -> None:
    """Invalid fields are a validation error before the database is asked."""
    with pytest.raises(ValidationError, match="initials: String should match"):
        users.add(email="new@b.de", initials="N1")

    taken["email"].assert_not_called()
    taken["backend_add"].assert_not_called()


@pytest.mark.parametrize(
    ("email_taken", "initials_taken", "errors"),
    [
        (True, False, ["A user with email 'new@b.de' already exists."]),
        (False, True, ["A user with initials 'NB' already exists."]),
        (
            True,
            True,
            [
                "A user with email 'new@b.de' already exists.",
                "A user with initials 'NB' already exists.",
            ],
        ),
    ],
)
def test_add_rejects_duplicates(
    taken: dict[str, MagicMock],
    errors: list[str],
    *,
    email_taken: bool,
    initials_taken: bool,
) -> None:
    """A taken email and taken initials are validation errors, nothing is stored."""
    taken["email"].return_value = email_taken
    taken["initials"].return_value = initials_taken

    with pytest.raises(ValidationError) as excinfo:
        users.add(email="New@b.de", initials="NB")

    assert excinfo.value.errors == errors
    taken["backend_add"].assert_not_called()


def test_update_cleans_fields_and_ignores_own_initials(
    taken: dict[str, MagicMock],
) -> None:
    """The fields are cleaned, the user itself is excluded from the uniqueness check."""
    users.update("a@b.de", initials=" AB ", messenger_id=" U123 ")

    taken["initials"].assert_called_once_with("AB", except_email="a@b.de")
    taken["backend_update"].assert_called_once_with(
        "a@b.de", UserUpdate(initials="AB", messenger_id="U123")
    )


def test_update_rejects_initials_of_another_user(taken: dict[str, MagicMock]) -> None:
    """Initials of another user are a validation error, nothing is stored."""
    taken["initials"].return_value = True

    with pytest.raises(ValidationError, match="initials 'CD'"):
        users.update("a@b.de", initials="CD", messenger_id=None)

    taken["backend_update"].assert_not_called()


def test_update_without_messenger_id_raises(taken: dict[str, MagicMock]) -> None:
    """An update that leaves the messenger id out does not clear it."""
    with pytest.raises(ValidationError, match="messenger_id: Field required"):
        users.update("a@b.de", initials="AB")

    taken["backend_update"].assert_not_called()


def test_update_rejects_invalid_initials(taken: dict[str, MagicMock]) -> None:
    """Invalid initials are a validation error also on update."""
    with pytest.raises(ValidationError, match="initials: String should match"):
        users.update("a@b.de", initials="toolonginitials", messenger_id=None)

    taken["backend_update"].assert_not_called()


@patch("shared.db._mongo.users.models")
def test_set_status(mock_models: MagicMock) -> None:
    """The status of the user with the given email is set."""
    users.set_status("a@b.de", UserStatus.INACTIVE)

    mock_models.User.objects.assert_called_once_with(email="a@b.de")
    mock_models.User.objects.return_value.update_one.assert_called_once_with(
        status=UserStatus.INACTIVE
    )


@patch("shared.db._mongo.users.models")
def test_set_status_of_unknown_user_raises(mock_models: MagicMock) -> None:
    """Setting the status of a user that does not exist is an error."""
    mock_models.User.objects.return_value.update_one.return_value = 0

    with pytest.raises(NotFoundError, match="a@b.de"):
        users.set_status("a@b.de", UserStatus.INACTIVE)
