"""Tests for the leases API and its Mongo backend."""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from pymongo.errors import DuplicateKeyError, OperationFailure

from shared.db import leases
from shared.db._mongo.leases import LEASE_COLLECTION
from shared.domain.errors import DatabaseError

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
TTL = timedelta(minutes=3)


@pytest.fixture
def mock_collection() -> Iterator[MagicMock]:
    """Patch the lease collection."""
    db = MagicMock()
    with (
        patch("shared.db._mongo.leases.ensure_connected"),
        patch("shared.db._mongo.leases.get_db", return_value=db),
    ):
        yield db[LEASE_COLLECTION]


def test_try_acquire_claims_or_renews_expired_or_own_lease_atomically(
    mock_collection: MagicMock,
) -> None:
    """The lease is updated only if expired or held by the holder, and extended."""
    # given
    mock_collection.find_one_and_update.return_value = {"instance": "me"}

    # when
    acquired = leases.try_acquire("monitoring", "me", ttl=TTL, now=NOW)

    # then
    assert acquired is True
    query, update = mock_collection.find_one_and_update.call_args.args
    assert query == {
        "_id": "monitoring",
        "$or": [{"instance": "me"}, {"expires_at": {"$lt": NOW}}],
    }
    assert update == {"$set": {"instance": "me", "expires_at": NOW + TTL}}
    assert mock_collection.find_one_and_update.call_args.kwargs["upsert"] is True


def test_try_acquire_fails_when_held_by_another_holder(
    mock_collection: MagicMock,
) -> None:
    """A valid lease of another holder makes the upsert collide on the _id."""
    mock_collection.find_one_and_update.side_effect = DuplicateKeyError("dup")

    assert leases.try_acquire("monitoring", "me", ttl=TTL, now=NOW) is False


def test_try_acquire_fails_when_returned_lease_belongs_to_another_holder(
    mock_collection: MagicMock,
) -> None:
    """Holding the lease is derived from the holder in the returned document."""
    mock_collection.find_one_and_update.return_value = {"instance": "other"}

    assert leases.try_acquire("monitoring", "me", ttl=TTL, now=NOW) is False


def test_try_acquire_raises_when_lease_cannot_be_written(
    mock_collection: MagicMock,
) -> None:
    """Any other failure, e.g. missing permissions, is a database error for the caller to judge."""
    mock_collection.find_one_and_update.side_effect = OperationFailure(
        "not authorized on krakendb to execute command"
    )

    with pytest.raises(DatabaseError, match="not authorized"):
        leases.try_acquire("monitoring", "me", ttl=TTL, now=NOW)
