"""Unit tests for the acquisition monitor plugin."""

from unittest.mock import MagicMock, patch

from common.keys import DagContext, DagParams
from plugins.sensors.acquisition_monitor import AcquisitionMonitor


def get_sensor() -> AcquisitionMonitor:
    """Get an instance of the sensor."""
    return AcquisitionMonitor(
        task_id="some_task_id", instrument_id="some_instrument_id"
    )


@patch("plugins.sensors.acquisition_monitor.RawDataWrapper")
def test_poke_file(
    mock_raw_data_wrapper: MagicMock,
) -> None:
    """Test poke method correctly return file status."""
    mock_path = MagicMock()
    attempts_with_changing_size = [
        0,
        1,
    ]
    attempts_with_constant_size = [100] * 5
    mock_path.stat.side_effect = [
        MagicMock(st_size=size)
        for size in attempts_with_changing_size + attempts_with_constant_size
    ]
    mock_raw_data_wrapper.create.return_value.file_path_to_watch.return_value = (
        mock_path
    )

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_NAME: "some_file.raw"}})

    # when
    for _ in range(len(attempts_with_changing_size + attempts_with_constant_size) - 1):
        result = sensor.poke({})
        assert not result
    result = sensor.poke({})
    assert result
