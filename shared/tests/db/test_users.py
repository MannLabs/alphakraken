"""Tests for the users API and its Mongo backend."""

from collections.abc import Iterator
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest

from shared.db import models, users
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
