"""Unit tests for the file sensor plugin."""

# ruff: noqa: SLF001 # Private member accessed
from pathlib import Path
from unittest.mock import MagicMock, patch

from plugins.sensors.file_sensor import FileCreationSensor

SOME_INSTRUMENT_ID = "some_instrument_id"


def get_sensor() -> FileCreationSensor:
    """Get an instance of the sensor."""
    return FileCreationSensor(task_id="some_task_id", instrument_id=SOME_INSTRUMENT_ID)


@patch.dict(
    "plugins.sensors.file_sensor.INSTRUMENTS",
    {SOME_INSTRUMENT_ID: {"raw_data_path": "apc_tims_1"}},
)
@patch("plugins.sensors.file_sensor.Observer")
@patch("plugins.sensors.file_sensor.FileCreationEventHandler")
def test_poke_file_not_created(
    mock_event_handler: MagicMock, mock_observer: MagicMock
) -> None:
    """Test poke method when file is not created and observer not alive."""
    # given
    mock_event_handler.return_value.file_created = False
    mock_observer.return_value.is_alive.return_value = False

    ti = MagicMock()

    # when
    sensor = get_sensor()
    result = sensor.poke({"task_instance": ti})

    # then
    assert not result
    assert sensor._path_to_watch == Path("/opt/airflow/acquisition_pcs/apc_tims_1")
    mock_observer.return_value.schedule.assert_called_once()
    mock_observer.return_value.start.assert_called_once()
    mock_observer.return_value.stop.assert_not_called()
    mock_observer.return_value.join.assert_not_called()


@patch.dict(
    "plugins.sensors.file_sensor.INSTRUMENTS",
    {SOME_INSTRUMENT_ID: {"raw_data_path": "apc_tims_1"}},
)
@patch("plugins.sensors.file_sensor.Observer")
@patch("plugins.sensors.file_sensor.FileCreationEventHandler")
def test_poke_file_created(
    mock_event_handler: MagicMock,
    mock_observer: MagicMock,
) -> None:
    """Test poke method when file is created and observer alive."""
    # given
    mock_event_handler.return_value.file_created = True
    mock_observer.return_value.is_alive.return_value = True

    ti = MagicMock()

    # when
    sensor = get_sensor()
    result = sensor.poke({"task_instance": ti})

    # then
    assert result
    mock_observer.return_value.schedule.assert_not_called()
    mock_observer.return_value.start.assert_not_called()
    mock_observer.return_value.stop.assert_called_once()
    mock_observer.return_value.join.assert_called_once()
