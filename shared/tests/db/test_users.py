"""Tests for the users API and its Mongo backend."""

from collections.abc import Iterator
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from shared.db import models, users
from shared.db._mongo import users as backend
from shared.domain.errors import ValidationError
from shared.domain.users import UserStatus
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
    with patch("shared.db._mongo.users.ensure_connected"):
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
            email="masc@example.org", initials="MaSc", messenger_id="U_MASC"
        )

    mock_models.User.assert_called_once_with(
        email="masc@example.org", initials="MaSc", messenger_id="U_MASC"
    )
    mock_save.assert_called_once_with(force_insert=True)
    assert result == user(created_at_=AWARE_TIME)


@patch("shared.db._mongo.users.models")
def test_backend_update(mock_models: MagicMock) -> None:
    """The backend saves the document, to run its field validation."""
    backend.update("a@b.de", initials="NB", messenger_id="U123")

    mock_models.User.objects.get.assert_called_once_with(email="a@b.de")
    document = mock_models.User.objects.get.return_value
    assert document.initials == "NB"
    assert document.messenger_id == "U123"
    document.save.assert_called_once()


EXISTING = [
    user(email="a@b.de", initials="AB"),
    user(email="c@d.de", initials="CD", status=UserStatus.INACTIVE),
]


@patch("shared.db._mongo.users.add")
@patch("shared.db.users.find", return_value=EXISTING)
def test_add_normalizes_email_and_checks_all_users(
    mock_find: MagicMock, mock_backend_add: MagicMock
) -> None:
    """A user with unused email and initials is added with the email in lower case."""
    result = users.add(email=" New@B.de ", initials="NB", messenger_id="U123")

    mock_find.assert_called_once_with(include_inactive=True)
    mock_backend_add.assert_called_once_with(
        email="new@b.de", initials="NB", messenger_id="U123"
    )
    assert result == mock_backend_add.return_value


@pytest.mark.parametrize(
    ("email", "initials", "error"),
    [
        ("A@b.de", "NB", "email 'a@b.de'"),
        ("new@b.de", "AB", "initials 'AB'"),
        # inactive users keep their email and initials
        ("new@b.de", "CD", "initials 'CD'"),
    ],
)
@patch("shared.db._mongo.users.add")
@patch("shared.db.users.find", return_value=EXISTING)
def test_add_rejects_duplicates(
    mock_find: MagicMock,  # noqa: ARG001
    mock_backend_add: MagicMock,
    email: str,
    initials: str,
    error: str,
) -> None:
    """A taken email or taken initials are a validation error, nothing is stored."""
    with pytest.raises(ValidationError, match=error) as excinfo:
        users.add(email=email, initials=initials)

    assert len(excinfo.value.errors) == 1
    mock_backend_add.assert_not_called()


@patch("shared.db._mongo.users.update")
@patch("shared.db.users.find", return_value=EXISTING)
def test_update_may_keep_own_initials(
    mock_find: MagicMock,  # noqa: ARG001
    mock_backend_update: MagicMock,
) -> None:
    """The user itself is excluded from the uniqueness check."""
    users.update("a@b.de", initials="AB", messenger_id="U123")

    mock_backend_update.assert_called_once_with(
        "a@b.de", initials="AB", messenger_id="U123"
    )


@patch("shared.db._mongo.users.update")
@patch("shared.db.users.find", return_value=EXISTING)
def test_update_rejects_initials_of_another_user(
    mock_find: MagicMock,  # noqa: ARG001
    mock_backend_update: MagicMock,
) -> None:
    """Initials of another user are a validation error, nothing is stored."""
    with pytest.raises(ValidationError, match="initials 'CD'"):
        users.update("a@b.de", initials="CD", messenger_id=None)

    mock_backend_update.assert_not_called()


@patch("shared.db._mongo.users.models")
def test_set_status(mock_models: MagicMock) -> None:
    """The status of the user with the given email is set."""
    users.set_status("a@b.de", UserStatus.INACTIVE)

    mock_models.User.objects.assert_called_once_with(email="a@b.de")
    mock_models.User.objects.return_value.update_one.assert_called_once_with(
        status=UserStatus.INACTIVE
    )
