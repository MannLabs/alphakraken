"""Unit tests for the active instance election."""

from collections.abc import Generator
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
import pytz
from pymongo.errors import DuplicateKeyError, OperationFailure

from monitoring.active_instance import (
    ACTIVE_INSTANCE_COLLECTION,
    ACTIVE_INSTANCE_DOCUMENT_ID,
    ActiveInstanceElection,
)

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
TIMEOUT = timedelta(minutes=3)


@pytest.fixture
def mock_collection() -> Generator[MagicMock, None, None]:
    """Patch the active instance collection."""
    db = MagicMock()
    with patch("monitoring.active_instance.get_db", return_value=db):
        yield db[ACTIVE_INSTANCE_COLLECTION]


def test_try_become_active_should_claim_or_renew_expired_or_own_claim_atomically(
    mock_collection: MagicMock,
) -> None:
    """The claim is updated only if expired or held by this instance, and extended."""
    # given
    mock_collection.find_one_and_update.return_value = {"instance": "me"}

    # when
    is_active = ActiveInstanceElection("me", TIMEOUT).try_become_active(NOW)

    # then
    assert is_active is True
    query, update = mock_collection.find_one_and_update.call_args.args
    assert query == {
        "_id": ACTIVE_INSTANCE_DOCUMENT_ID,
        "$or": [{"instance": "me"}, {"expires_at": {"$lt": NOW}}],
    }
    assert update == {"$set": {"instance": "me", "expires_at": NOW + TIMEOUT}}
    assert mock_collection.find_one_and_update.call_args.kwargs["upsert"] is True


def test_try_become_active_should_fail_when_claimed_by_other_instance(
    mock_collection: MagicMock,
) -> None:
    """A valid claim of another instance makes the upsert collide on the _id."""
    # given
    mock_collection.find_one_and_update.side_effect = DuplicateKeyError("dup")

    # when / then
    assert ActiveInstanceElection("me", TIMEOUT).try_become_active(NOW) is False


def test_try_become_active_should_fail_when_returned_claim_belongs_to_other_instance(
    mock_collection: MagicMock,
) -> None:
    """The active role is derived from the instance in the returned claim."""
    # given
    mock_collection.find_one_and_update.return_value = {"instance": "other"}

    # when / then
    assert ActiveInstanceElection("me", TIMEOUT).try_become_active(NOW) is False


def test_try_become_active_should_fail_open_when_claim_cannot_be_written(
    mock_collection: MagicMock,
) -> None:
    """Without permission to write the claim, every instance runs the checks rather than none."""
    # given
    mock_collection.find_one_and_update.side_effect = OperationFailure(
        "not authorized on krakendb to execute command"
    )

    # when / then
    assert ActiveInstanceElection("me", TIMEOUT).try_become_active(NOW) is True
