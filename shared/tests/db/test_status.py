"""Tests for the status API and its Mongo backend."""

from unittest.mock import MagicMock, patch

from shared.db import models
from shared.db.status import instrument_ids
from shared.domain.status import KrakenStatusEntities


@patch("shared.db._mongo.status.models")
def test_instrument_ids(mock_models: MagicMock) -> None:
    """The ids of the instrument status entries are returned."""
    mock_models.KrakenStatus.objects.return_value.only.return_value = [
        models.KrakenStatus(id="inst1"),
        models.KrakenStatus(id="inst2"),
    ]

    with patch("shared.db._mongo.status.ensure_connected"):
        assert instrument_ids() == ["inst1", "inst2"]
    mock_models.KrakenStatus.objects.assert_called_once_with(
        entity_type=KrakenStatusEntities.INSTRUMENT
    )
