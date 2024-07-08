"""Unit tests for the acquisition monitor plugin."""

from unittest.mock import MagicMock, patch

from common.keys import DagContext, DagParams
from plugins.sensors.acquisition_monitor import (
    SIZE_CHECK_INTERVAL_M,
    AcquisitionMonitor,
)


def get_sensor() -> AcquisitionMonitor:
    """Get an instance of the sensor."""
    return AcquisitionMonitor(
        task_id="some_task_id", instrument_id="some_instrument_id"
    )


@patch("plugins.sensors.acquisition_monitor.RawDataWrapper")
def test_poke_file_dir_contents_change_file_is_added(
    mock_raw_data_wrapper: MagicMock,
) -> None:
    """Test poke method correctly return when dir contents change (file is added)."""
    mock_path = MagicMock()
    mock_path.stat.return_value = MagicMock(st_size=1)

    mock_raw_data_wrapper.create.return_value.file_path_to_watch.return_value = (
        mock_path
    )

    mock_raw_data_wrapper.create.return_value.get_raw_files_on_instrument.side_effect = [
        {"some_file.raw"},  # initial content (pre_execute)
        {"some_file.raw"},  # first poke
        {"some_file.raw", "some_new_file.raw"},  # second poke
    ]

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_NAME: "some_file.raw"}})

    # when
    result = sensor.poke({})
    assert not result

    result = sensor.poke({})
    assert result


@patch("plugins.sensors.acquisition_monitor.RawDataWrapper")
def test_poke_file_dir_contents_change_file_is_removed(
    mock_raw_data_wrapper: MagicMock,
) -> None:
    """Test poke method correctly returns when dir contents change (file is removed)."""
    mock_path = MagicMock()
    mock_path.stat.return_value = MagicMock(st_size=1)

    mock_raw_data_wrapper.create.return_value.file_path_to_watch.return_value = (
        mock_path
    )

    mock_raw_data_wrapper.create.return_value.get_raw_files_on_instrument.side_effect = [
        {"some_file.raw", "some_file2.raw"},  # initial content (pre_execute)
        {"some_file.raw", "some_file2.raw"},  # first poke
        {"some_file.raw"},  # second poke
    ]

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_NAME: "some_file.raw"}})

    # when
    result = sensor.poke({})
    assert not result

    result = sensor.poke({})
    assert not result


@patch("plugins.sensors.acquisition_monitor.RawDataWrapper")
@patch("plugins.sensors.acquisition_monitor.AcquisitionMonitor._get_timestamp")
def test_poke_file_dir_contents_dont_change_but_file_is_unchanged(
    mock_get_timestamp: MagicMock,
    mock_raw_data_wrapper: MagicMock,
) -> None:
    """Test poke method correctly return file status when dir contents do not change and file also does not."""
    mock_path = MagicMock()
    mock_raw_data_wrapper.create.return_value.file_path_to_watch.return_value = (
        mock_path
    )
    mock_raw_data_wrapper.create.return_value.get_raw_files_on_instrument.return_value = set()  # this stays constant

    mock_get_timestamp.side_effect = [
        1,  # pre_execute (initial time stamp)
        2,  # first poke
        SIZE_CHECK_INTERVAL_M * 60 + 2,  # second poke, first file check
        SIZE_CHECK_INTERVAL_M * 60 + 3,  # third poke
        2 * SIZE_CHECK_INTERVAL_M * 60 + 3,  # fourth poke, second file check
    ]
    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_NAME: "some_file.raw"}})

    # when
    for _ in range(3):
        result = sensor.poke({})
        assert not result
    result = sensor.poke({})
    assert result

    assert mock_path.stat.call_count == 2  # noqa: PLR2004
