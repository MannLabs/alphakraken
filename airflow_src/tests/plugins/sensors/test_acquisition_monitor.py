"""Unit tests for the acquisition monitor plugin."""

# ruff: noqa: SLF001

from unittest.mock import MagicMock, call, patch

from common.keys import DagContext, DagParams
from plugins.sensors.acquisition_monitor import (
    SIZE_CHECK_INTERVAL_M,
    SOFT_TIMEOUT_ON_MISSING_MAIN_FILE_M,
    AcquisitionMonitor,
)


def get_sensor() -> AcquisitionMonitor:
    """Get an instance of the sensor."""
    return AcquisitionMonitor(
        task_id="some_task_id", instrument_id="some_instrument_id"
    )


@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
def test_poke_file_dir_contents_change_file_is_added(
    mock_update_raw_file: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test poke method correctly return when dir contents change (file is added)."""
    mock_path = MagicMock()
    mock_path.stat.return_value = MagicMock(st_size=1)

    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.file_path_to_monitor_acquisition.return_value = mock_path

    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.get_raw_files_on_instrument.side_effect = [
        {"some_file.raw"},  # initial content (pre_execute)
        {"some_file.raw"},  # first poke
        {"some_file.raw", "some_new_file.raw"},  # second poke
    ]

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    result = sensor.poke({})
    assert not result

    result = sensor.poke({})
    assert result

    mock_update_raw_file.assert_called_once_with(
        "some_file.raw", new_status="monitoring_acquisition"
    )


@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
def test_poke_file_dir_contents_change_two_files_are_added(
    mock_update_raw_file: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test poke method correctly returns when dir contents change (two files are added)."""
    mock_path = MagicMock()
    mock_path.stat.return_value = MagicMock(st_size=1)

    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.file_path_to_monitor_acquisition.return_value = mock_path

    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.get_raw_files_on_instrument.side_effect = [
        {"some_file.raw"},  # initial content (pre_execute)
        {"some_file.raw"},  # first poke
        {
            "some_file.raw",
            "some_new_file_1.raw",
            "some_new_file_2.raw",
        },  # second poke -> two new files
        {
            "some_file.raw",
            "some_new_file_1.raw",
            "some_new_file_2.raw",
            "some_new_file_3.raw",
        },  # third poke -> one new file
    ]

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    result = sensor.poke({})
    assert not result

    # two files -> false
    result = sensor.poke({})
    assert not result

    result = sensor.poke({})
    assert result

    mock_update_raw_file.assert_called_once_with(
        "some_file.raw", new_status="monitoring_acquisition"
    )


@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
def test_poke_file_dir_contents_change_file_is_removed(
    mock_update_raw_file: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test poke method correctly returns when dir contents change (file is removed)."""
    mock_path = MagicMock()
    mock_path.stat.return_value = MagicMock(st_size=1)

    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.file_path_to_monitor_acquisition.return_value = mock_path

    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.get_raw_files_on_instrument.side_effect = [
        {"some_file.raw", "some_file2.raw"},  # initial content (pre_execute)
        {"some_file.raw", "some_file2.raw"},  # first poke
        {"some_file.raw"},  # second poke
    ]

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    result = sensor.poke({})
    assert not result

    result = sensor.poke({})
    assert not result

    mock_update_raw_file.assert_called_once_with(
        "some_file.raw", new_status="monitoring_acquisition"
    )


@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
def test_poke_file_dir_contents_change_main_file_does_not_exist(
    mock_update_raw_file: MagicMock,  # noqa: ARG001
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test poke method correctly returns when dir contents change (file is removed)."""
    mock_path = MagicMock()
    mock_path.stat.return_value = MagicMock(st_size=1)

    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.file_path_to_monitor_acquisition.return_value.exists.return_value = False

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    result = sensor.poke({})
    assert not result
    assert not sensor._main_file_exists


@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.get_timestamp")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
def test_poke_file_dir_contents_change_main_file_does_not_exist_for_too_long(
    mock_update_raw_file: MagicMock,  # noqa: ARG001
    mock_get_timestamp: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test poke method correctly returns when dir contents change (file is removed)."""
    mock_path = MagicMock()
    mock_path.stat.return_value = MagicMock(st_size=1)

    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.file_path_to_monitor_acquisition.return_value.exists.return_value = False

    mock_get_timestamp.side_effect = [
        1,  # pre_execute (initial time stamp)
        2,  # first poke
        3,  # second poke
        SOFT_TIMEOUT_ON_MISSING_MAIN_FILE_M * 60 + 3,  # third poke => too long
    ]

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    sensor.poke({})
    sensor.poke({})
    result = sensor.poke({})
    assert result

    assert not sensor._main_file_exists


@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.get_timestamp")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
def test_poke_file_dir_contents_dont_change_but_file_is_unchanged(
    mock_update_raw_file: MagicMock,
    mock_get_timestamp: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test poke method correctly return file status when dir contents do not change and file also does not."""
    mock_path = MagicMock()
    mock_path.stat.return_value.st_size = 100
    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.file_path_to_monitor_acquisition.return_value = mock_path
    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.get_raw_files_on_instrument.return_value = set()  # this stays constant

    mock_get_timestamp.side_effect = [
        1,  # pre_execute (initial time stamp)
        2,  # first poke
        SIZE_CHECK_INTERVAL_M * 60 + 2,  # second poke, first file check
        SIZE_CHECK_INTERVAL_M * 60 + 3,  # third poke
        2 * SIZE_CHECK_INTERVAL_M * 60 + 3,  # fourth poke, second file check
    ]
    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    for _ in range(3):
        result = sensor.poke({})
        assert not result
    result = sensor.poke({})
    assert result

    assert mock_path.stat.call_count == 2  # noqa: PLR2004

    mock_update_raw_file.assert_called_once_with(
        "some_file.raw", new_status="monitoring_acquisition"
    )


@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.put_xcom")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
def test_post_execute_ok(
    mock_update_raw_file: MagicMock,
    mock_put_xcom: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test post_execute correctly works if no acquisition errors."""
    mock_path = MagicMock()

    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.file_path_to_monitor_acquisition.return_value = mock_path

    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.get_raw_files_on_instrument.side_effect = [
        {"some_file.raw"},  # initial content (pre_execute)
    ]

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})
    sensor._main_file_exists = True

    ti = MagicMock()
    # when
    sensor.post_execute({"ti": ti}, result=True)

    mock_put_xcom.assert_called_once_with(ti, "acquisition_monitor_errors", [])

    mock_update_raw_file.assert_has_calls(
        [
            call("some_file.raw", new_status="monitoring_acquisition"),
            call("some_file.raw", new_status="monitoring_done"),
        ]
    )


@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.put_xcom")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
def test_post_execute_acquisition_errors(
    mock_update_raw_file: MagicMock,
    mock_put_xcom: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test post_execute correctly works if no acquisition errors."""
    mock_path = MagicMock()

    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.file_path_to_monitor_acquisition.return_value = mock_path

    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.get_raw_files_on_instrument.return_value = (
        {"some_file.raw"},
    )  # initial content (pre_execute)

    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.main_file_name = (
        "analysis.tdf_bin"
    )

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})
    sensor._main_file_exists = False

    ti = MagicMock()

    # when
    sensor.post_execute({"ti": ti}, result=True)

    mock_put_xcom.assert_called_once_with(
        ti,
        "acquisition_monitor_errors",
        ["Main file was not created: analysis.tdf_bin"],
    )

    mock_update_raw_file.assert_has_calls(
        [
            call("some_file.raw", new_status="monitoring_acquisition"),
            call("some_file.raw", new_status="monitoring_done"),
        ]
    )
