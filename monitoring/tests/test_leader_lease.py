"""Unit tests for the leader lease."""

from collections.abc import Generator
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
import pytz
from pymongo.errors import DuplicateKeyError

from monitoring.leader_lease import LEASE_COLLECTION, LEASE_ID, LeaderLease

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
LEASE_DURATION = timedelta(minutes=3)


@pytest.fixture
def mock_collection() -> Generator[MagicMock, None, None]:
    """Patch the lease collection and the current time."""
    db = MagicMock()
    with (
        patch("monitoring.leader_lease.get_db", return_value=db),
        patch("monitoring.leader_lease.datetime") as mock_datetime,
    ):
        mock_datetime.now.return_value = NOW
        yield db[LEASE_COLLECTION]


def test_try_acquire_should_take_or_renew_free_or_own_lease_atomically(
    mock_collection: MagicMock,
) -> None:
    """The lease is updated only if free or held by this instance, and extended."""
    # given
    mock_collection.find_one_and_update.return_value = {"holder": "me"}

    # when
    is_leader = LeaderLease("me", LEASE_DURATION).try_acquire()

    # then
    assert is_leader is True
    query, update = mock_collection.find_one_and_update.call_args.args
    assert query == {
        "_id": LEASE_ID,
        "$or": [{"holder": "me"}, {"expires_at": {"$lt": NOW}}],
    }
    assert update == {"$set": {"holder": "me", "expires_at": NOW + LEASE_DURATION}}
    assert mock_collection.find_one_and_update.call_args.kwargs["upsert"] is True


def test_try_acquire_should_fail_when_lease_held_by_other_instance(
    mock_collection: MagicMock,
) -> None:
    """A valid lease of another instance makes the upsert collide on the _id."""
    # given
    mock_collection.find_one_and_update.side_effect = DuplicateKeyError("dup")

    # when / then
    assert LeaderLease("me", LEASE_DURATION).try_acquire() is False


def test_try_acquire_should_fail_when_returned_lease_belongs_to_other_instance(
    mock_collection: MagicMock,
) -> None:
    """Leadership is derived from the returned lease holder."""
    # given
    mock_collection.find_one_and_update.return_value = {"holder": "other"}

    # when / then
    assert LeaderLease("me", LEASE_DURATION).try_acquire() is False
