"""Unit tests for the active instance election."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
import pytz

from monitoring.active_instance import ACTIVE_INSTANCE_LEASE, ActiveInstanceElection
from shared.domain.errors import DatabaseError

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=pytz.UTC)
TIMEOUT = timedelta(minutes=3)


@pytest.mark.parametrize("acquired", [True, False])
@patch("shared.db.leases.try_acquire")
def test_try_become_active_should_follow_the_lease(
    mock_try_acquire: MagicMock, *, acquired: bool
) -> None:
    """The instance is active exactly if it holds the lease, claimed for the timeout."""
    # given
    mock_try_acquire.return_value = acquired

    # when
    is_active = ActiveInstanceElection("me", TIMEOUT).try_become_active(NOW)

    # then
    assert is_active is acquired
    mock_try_acquire.assert_called_once_with(
        ACTIVE_INSTANCE_LEASE, "me", ttl=TIMEOUT, now=NOW
    )


@patch("shared.db.leases.try_acquire", side_effect=DatabaseError("not authorized"))
def test_try_become_active_should_fail_open_when_lease_cannot_be_written(
    mock_try_acquire: MagicMock,  # noqa: ARG001
) -> None:
    """Without permission to write the lease, every instance runs the checks rather than none."""
    assert ActiveInstanceElection("me", TIMEOUT).try_become_active(NOW) is True
