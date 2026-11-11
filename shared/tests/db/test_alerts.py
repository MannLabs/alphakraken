"""Tests for the alerts API and its Mongo backend."""

from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from bson import ObjectId

from shared.db import alerts
from shared.db._mongo import models
from shared.tests.factories import alert_issue, alert_suppression

NAIVE_TIME = datetime(2024, 1, 2, 3, 4, 5)  # noqa: DTZ001 as stored by pymongo
AWARE_TIME = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)
OBJECT_ID = ObjectId("65a0000000000000000000aa")


@pytest.fixture(autouse=True)
def _connected() -> Iterator[None]:
    with patch("shared.db._mongo.alerts.ensure_connected"):
        yield


@patch("shared.db._mongo.alerts.models")
def test_find_issues(mock_models: MagicMock) -> None:
    """Issues are mapped with a string id and UTC-aware datetimes."""
    mock_models.AlertIssue.objects = [
        models.AlertIssue(
            id=OBJECT_ID, alert="DiskSpaceAlert", key="inst1", first_seen=NAIVE_TIME
        )
    ]

    assert alerts.find_issues() == [
        alert_issue(id=str(OBJECT_ID), first_seen=AWARE_TIME, last_notified=None)
    ]


@patch("shared.db._mongo.alerts.models")
def test_add_issue(mock_models: MagicMock) -> None:
    """A new issue is inserted, never overwriting an existing one, and returned."""
    document = models.AlertIssue(
        id=OBJECT_ID, alert="DiskSpaceAlert", key="inst1", first_seen=AWARE_TIME
    )
    mock_models.AlertIssue.return_value = document

    with patch.object(models.AlertIssue, "save") as mock_save:
        result = alerts.add_issue(
            alert="DiskSpaceAlert", key="inst1", first_seen=AWARE_TIME
        )

    mock_models.AlertIssue.assert_called_once_with(
        alert="DiskSpaceAlert", key="inst1", first_seen=AWARE_TIME
    )
    mock_save.assert_called_once_with(force_insert=True)
    assert result == alert_issue(id=str(OBJECT_ID), first_seen=AWARE_TIME)


@patch("shared.db._mongo.alerts.models")
def test_set_issue_notified(mock_models: MagicMock) -> None:
    """The notification time is set on the issue with the given id."""
    alerts.set_issue_notified("some_id", AWARE_TIME)

    mock_models.AlertIssue.objects.assert_called_once_with(id="some_id")
    mock_models.AlertIssue.objects.return_value.update_one.assert_called_once_with(
        set__last_notified=AWARE_TIME
    )


@pytest.mark.parametrize("active_only", [True, False])
@patch("shared.db._mongo.alerts.models")
def test_find_suppressions(mock_models: MagicMock, *, active_only: bool) -> None:
    """Suppressions come newest first; expired ones are filtered out on request."""
    document = models.AlertSuppression(
        id=OBJECT_ID, comment="comment", created_at_=NAIVE_TIME
    )
    query = mock_models.AlertSuppression.objects.order_by.return_value
    query.filter.return_value = [document]
    query.__iter__.return_value = [document]

    result = alerts.find_suppressions(active_only=active_only)

    assert result == [alert_suppression(id=str(OBJECT_ID), created_at_=AWARE_TIME)]
    mock_models.AlertSuppression.objects.order_by.assert_called_once_with(
        "-created_at_"
    )
    assert query.filter.called is active_only


@patch("shared.db._mongo.alerts.models")
def test_add_suppression(mock_models: MagicMock) -> None:
    """A new suppression is saved with the given fields."""
    fields: dict[str, Any] = {
        "alert": "PumpPressureAlert",
        "instrument_id": "astral1",
        "identifier": "*",
        "details": "*",
        "comment": "maintenance",
        "expires_at": AWARE_TIME,
        "created_by": "me",
    }

    alerts.add_suppression(**fields)

    mock_models.AlertSuppression.assert_called_once_with(**fields)
    mock_models.AlertSuppression.return_value.save.assert_called_once()


@patch("shared.db._mongo.alerts.models")
def test_delete_suppression(mock_models: MagicMock) -> None:
    """The suppression with the given id is deleted."""
    alerts.delete_suppression("abc")

    mock_models.AlertSuppression.objects.assert_called_once_with(id="abc")
    mock_models.AlertSuppression.objects.return_value.delete.assert_called_once()
