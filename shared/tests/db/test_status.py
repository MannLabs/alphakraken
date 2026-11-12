"""Tests for the status API and its Mongo backend."""

from datetime import datetime
from unittest.mock import MagicMock, patch

from shared.db._mongo import models
from shared.db.status import find, instrument_ids, update
from shared.domain.status import KrakenStatusEntities, KrakenStatusValues
from shared.tests.factories import kraken_status


@patch("shared.db._mongo.status.models")
def test_instrument_ids(mock_models: MagicMock) -> None:
    """The ids of the instrument status entries are returned."""
    mock_models.KrakenStatus.objects.return_value.only.return_value = [
        models.KrakenStatus(id="inst1"),
        models.KrakenStatus(id="inst2"),
    ]

    with patch("shared.db._mongo.connection.ensure_connected"):
        assert instrument_ids() == ["inst1", "inst2"]
    mock_models.KrakenStatus.objects.assert_called_once_with(
        entity_type=KrakenStatusEntities.INSTRUMENT
    )


@patch("shared.db._mongo.status.models")
def test_find(mock_models: MagicMock) -> None:
    """All status entries are mapped with UTC-aware datetimes."""
    mock_models.KrakenStatus.objects = [
        models.KrakenStatus(
            id="inst1",
            status=KrakenStatusValues.OK,
            free_space_gb=100,
            entity_type=KrakenStatusEntities.INSTRUMENT,
            updated_at_=datetime(2024, 1, 1, 12, 0, 0),  # noqa: DTZ001 as stored by pymongo
        )
    ]

    with patch("shared.db._mongo.connection.ensure_connected"):
        assert find() == [kraken_status()]


@patch("shared.db._mongo.status.datetime")
@patch("shared.db._mongo.status.models")
def test_update(mock_models: MagicMock, mock_datetime: MagicMock) -> None:
    """The status entry is saved with the current time, replacing an existing one."""
    with patch("shared.db._mongo.connection.ensure_connected"):
        update(
            "inst1",
            status=KrakenStatusValues.ERROR,
            status_details="details",
            free_space_gb=123,
        )

    mock_models.KrakenStatus.assert_called_once_with(
        id="inst1",
        status=KrakenStatusValues.ERROR,
        updated_at_=mock_datetime.now.return_value,
        free_space_gb=123,
        status_details="details",
        entity_type=KrakenStatusEntities.INSTRUMENT,
    )
    mock_models.KrakenStatus.return_value.save.assert_called_once()
