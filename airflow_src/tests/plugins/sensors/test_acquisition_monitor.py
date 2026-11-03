"""Unit tests for the acquisition monitor plugin."""

from pathlib import Path

# ruff: noqa: SLF001
from unittest.mock import MagicMock, PropertyMock, call, patch

import pytest
from common.keys import DagContext, DagParams
from plugins.sensors.acquisition_monitor import (
    FILES_SETTLE_TIME_S,
    FILES_UNCHANGED_TIMEOUT_M,
    FINALIZATION_TIMEOUT_M,
    OLD_FILE_THRESHOLD_H,
    SOFT_TIMEOUT_ON_MISSING_MAIN_FILE_M,
    ZENO_ZT_SCAN_FILE_THRESHOLD_BYTES,
    AcquisitionMonitor,
)
from raw_file_wrapper_factory import SciexRawFileMonitorWrapper

FILES_UNCHANGED_TIMEOUT_S = FILES_UNCHANGED_TIMEOUT_M * 60
FINALIZATION_TIMEOUT_S = FINALIZATION_TIMEOUT_M * 60


def get_sensor() -> AcquisitionMonitor:
    """Get an instance of the sensor."""
    return AcquisitionMonitor(
        task_id="some_task_id", instrument_id="some_instrument_id"
    )


def _setup_monitor_wrapper(
    mock_raw_file_wrapper_factory: MagicMock, *associated_paths: MagicMock
) -> MagicMock:
    """Set up the mocked monitor wrapper with the given associated files, the first one being the main file."""
    mock_monitor_wrapper = (
        mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value
    )
    if associated_paths:
        mock_monitor_wrapper.main_file_path.return_value = associated_paths[0]
    mock_monitor_wrapper.get_associated_files.return_value = list(associated_paths)
    mock_monitor_wrapper.is_corrupted_file_name.return_value = False
    return mock_monitor_wrapper


def _mock_path(size: int = 1, mtime: float = 10.0) -> MagicMock:
    """Get a mock path with a constant stat."""
    mock_path = MagicMock()
    mock_path.stat.return_value = MagicMock(st_size=size, st_mtime=mtime)
    return mock_path


@pytest.mark.parametrize("acquisition_finalized", [True, False])
@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.get_timestamp")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
@patch("plugins.sensors.acquisition_monitor.get_raw_file_by_id")
def test_poke_file_dir_contents_change_file_is_added(
    mock_get_raw_file_by_id: MagicMock,
    mock_update_raw_file: MagicMock,
    mock_get_timestamp: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
    *,
    acquisition_finalized: bool,
) -> None:
    """Test poke method returns True once a file is added, the associated files have settled and the raw file is finalized."""
    mock_get_raw_file_by_id.return_value.original_name = "some_file.raw"
    mock_monitor_wrapper = _setup_monitor_wrapper(
        mock_raw_file_wrapper_factory, _mock_path()
    )
    mock_monitor_wrapper.get_raw_files_on_instrument.side_effect = [
        {"some_file.raw"},  # initial content (pre_execute)
        {"some_file.raw"},  # first poke
        {"some_file.raw", "some_new_file.raw"},  # second poke
        {"some_file.raw", "some_new_file.raw"},  # third poke
    ]

    mock_get_timestamp.side_effect = [
        0,  # pre_execute
        1,  # first poke
        2,  # second poke: next file appeared
        2 + FILES_SETTLE_TIME_S,  # third poke: files settled
    ]

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    with patch.object(
        AcquisitionMonitor, "_acquisition_finalized", return_value=acquisition_finalized
    ):
        assert not sensor.poke({})
        assert not sensor.poke({})
        assert sensor.poke({}) == acquisition_finalized

    mock_update_raw_file.assert_called_once_with(
        mock_get_raw_file_by_id.return_value.id, new_status="monitoring_acquisition"
    )


@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.get_timestamp")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
@patch("plugins.sensors.acquisition_monitor.get_raw_file_by_id")
def test_poke_file_dir_contents_change_file_is_added_files_change_afterwards(
    mock_get_raw_file_by_id: MagicMock,
    mock_update_raw_file: MagicMock,  # noqa: ARG001
    mock_get_timestamp: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test poke method restarts the settle time if a non-main file changes after the next file appeared."""
    mock_get_raw_file_by_id.return_value.original_name = "some_file.raw"
    mock_other_path = MagicMock()
    mock_other_path.stat.side_effect = [
        MagicMock(st_size=1, st_mtime=10.0),  # first poke
        MagicMock(st_size=1, st_mtime=10.0),  # second poke
        MagicMock(st_size=2, st_mtime=11.0),  # third poke: still written to
        MagicMock(st_size=2, st_mtime=11.0),  # fourth poke
    ]
    mock_monitor_wrapper = _setup_monitor_wrapper(
        mock_raw_file_wrapper_factory, _mock_path(), mock_other_path
    )
    mock_monitor_wrapper.get_raw_files_on_instrument.side_effect = [
        {"some_file.raw"},  # initial content (pre_execute)
        {"some_file.raw"},  # first poke
        {"some_file.raw", "some_new_file.raw"},  # second poke
        {"some_file.raw", "some_new_file.raw"},  # third poke
        {"some_file.raw", "some_new_file.raw"},  # fourth poke
    ]

    mock_get_timestamp.side_effect = [
        0,  # pre_execute
        1,  # first poke
        2,  # second poke: next file appeared
        2 + FILES_SETTLE_TIME_S,  # third poke: would be settled, but files changed
        2 + 2 * FILES_SETTLE_TIME_S,  # fourth poke: files settled
    ]

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    assert not sensor.poke({})
    assert not sensor.poke({})
    assert not sensor.poke({})
    assert sensor.poke({})


@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
@patch("plugins.sensors.acquisition_monitor.get_raw_file_by_id")
@patch("plugins.sensors.acquisition_monitor.put_xcom")
def test_poke_file_dir_contents_change_corrupt_file_is_added(
    mock_put_xcom: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_update_raw_file: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test poke and post_execute methods correctly return when dir contents change (corrupted file is added)."""
    mock_get_raw_file_by_id.return_value.original_name = "some_file.raw"
    mock_monitor_wrapper = _setup_monitor_wrapper(
        mock_raw_file_wrapper_factory, _mock_path()
    )
    mock_monitor_wrapper.get_corrupted_file_name.return_value = (
        "some_file_CORRUPTED.raw"
    )
    mock_monitor_wrapper.get_raw_files_on_instrument.side_effect = [
        {"some_file.raw"},  # initial content (pre_execute)
        {"some_file.raw", "some_file_CORRUPTED.raw"},  # first poke
    ]

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    assert sensor.poke({})

    mock_update_raw_file.assert_called_once_with(
        mock_get_raw_file_by_id.return_value.id, new_status="monitoring_acquisition"
    )
    assert sensor._file_got_renamed

    ti = MagicMock()
    # when 2
    sensor.post_execute({"ti": ti}, result=True)

    mock_put_xcom.assert_called_once_with(
        ti, "acquisition_monitor_errors", ["File got renamed: some_file_CORRUPTED.raw"]
    )


@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
@patch("plugins.sensors.acquisition_monitor.get_raw_file_by_id")
@patch("plugins.sensors.acquisition_monitor.put_xcom")
def test_poke_file_dir_contents_change_corrupt_file_and_next_file_are_added(
    mock_put_xcom: MagicMock,
    mock_get_raw_file_by_id: MagicMock,
    mock_update_raw_file: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test poke and post_execute methods correctly return when a corrupted file appears alongside the next acquisition."""
    mock_get_raw_file_by_id.return_value.original_name = "some_file.raw"
    mock_monitor_wrapper = _setup_monitor_wrapper(
        mock_raw_file_wrapper_factory, _mock_path()
    )
    mock_monitor_wrapper.get_corrupted_file_name.return_value = (
        "some_file_CORRUPTED.raw"
    )
    mock_monitor_wrapper.get_raw_files_on_instrument.side_effect = [
        {"some_file.raw"},  # initial content (pre_execute)
        {
            "some_file_CORRUPTED.raw",
            "some_next_file.raw",
        },  # first poke -> renamed file plus already started next acquisition
    ]

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    assert sensor.poke({})

    mock_update_raw_file.assert_called_once_with(
        mock_get_raw_file_by_id.return_value.id, new_status="monitoring_acquisition"
    )
    assert sensor._file_got_renamed

    ti = MagicMock()
    # when 2
    sensor.post_execute({"ti": ti}, result=True)

    mock_put_xcom.assert_called_once_with(
        ti, "acquisition_monitor_errors", ["File got renamed: some_file_CORRUPTED.raw"]
    )


@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.get_timestamp")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
@patch("plugins.sensors.acquisition_monitor.get_raw_file_by_id")
def test_poke_file_dir_contents_change_corrupt_file_of_other_raw_file_is_added(
    mock_get_raw_file_by_id: MagicMock,
    mock_update_raw_file: MagicMock,  # noqa: ARG001
    mock_get_timestamp: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test poke method ignores a corrupted file of another raw file, but settles on a real next file."""
    mock_get_raw_file_by_id.return_value.original_name = "some_file.raw"
    mock_monitor_wrapper = _setup_monitor_wrapper(
        mock_raw_file_wrapper_factory, _mock_path()
    )
    mock_monitor_wrapper.get_corrupted_file_name.return_value = (
        "some_file_CORRUPTED.raw"
    )
    mock_monitor_wrapper.is_corrupted_file_name.side_effect = (
        lambda file_name: file_name.endswith("_CORRUPTED.raw")
    )
    mock_monitor_wrapper.get_raw_files_on_instrument.side_effect = [
        {"some_file.raw"},  # initial content (pre_execute)
        {"some_file.raw", "other_file_CORRUPTED.raw"},  # first poke
        {"some_file.raw", "other_file_CORRUPTED.raw"},  # second poke
        {
            "some_file.raw",
            "other_file_CORRUPTED.raw",
            "some_next_file.raw",
        },  # third poke
        {
            "some_file.raw",
            "other_file_CORRUPTED.raw",
            "some_next_file.raw",
        },  # fourth poke
    ]

    mock_get_timestamp.side_effect = [
        0,  # pre_execute
        1,  # first poke
        1
        + FILES_SETTLE_TIME_S,  # second poke: would be settled if the corrupted file counted
        10,  # third poke: next file appeared
        10 + FILES_SETTLE_TIME_S,  # fourth poke: files settled
    ]

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    assert not sensor.poke({})
    assert not sensor.poke({})
    assert not sensor.poke({})
    assert sensor.poke({})

    assert not sensor._file_got_renamed


@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.get_timestamp")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
@patch("plugins.sensors.acquisition_monitor.get_raw_file_by_id")
def test_poke_file_dir_contents_change_two_files_are_added(
    mock_get_raw_file_by_id: MagicMock,
    mock_update_raw_file: MagicMock,
    mock_get_timestamp: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test poke method correctly returns when dir contents change (two files are added)."""
    mock_get_raw_file_by_id.return_value.original_name = "some_file.raw"
    mock_monitor_wrapper = _setup_monitor_wrapper(
        mock_raw_file_wrapper_factory, _mock_path()
    )
    mock_monitor_wrapper.get_raw_files_on_instrument.side_effect = [
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
        {
            "some_file.raw",
            "some_new_file_1.raw",
            "some_new_file_2.raw",
            "some_new_file_3.raw",
        },  # fourth poke
    ]

    mock_get_timestamp.side_effect = [
        0,  # pre_execute
        1,  # first poke
        2,  # second poke
        3,  # third poke: next file appeared
        3 + FILES_SETTLE_TIME_S,  # fourth poke: files settled
    ]

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    assert not sensor.poke({})

    # two files -> false
    assert not sensor.poke({})

    # one new file -> settle time starts
    assert not sensor.poke({})

    assert sensor.poke({})

    mock_update_raw_file.assert_called_once_with(
        mock_get_raw_file_by_id.return_value.id, new_status="monitoring_acquisition"
    )


def _get_sensor_with_associated_files(*mock_paths: MagicMock) -> AcquisitionMonitor:
    """Get a sensor whose monitor wrapper lists the given paths as associated files."""
    sensor = get_sensor()
    sensor._raw_file = MagicMock()
    sensor._raw_file_monitor_wrapper = MagicMock()
    sensor._raw_file_monitor_wrapper.get_associated_files.return_value = list(
        mock_paths
    )
    return sensor


def test_update_files_stat() -> None:
    """Test _update_files_stat returns the time since the (size, mtime) of any associated file last changed."""
    mock_other_path = MagicMock()
    mock_other_path.stat.side_effect = [
        MagicMock(st_size=1, st_mtime=10.0),
        MagicMock(st_size=1, st_mtime=10.0),
        MagicMock(st_size=2, st_mtime=10.0),
        MagicMock(st_size=2, st_mtime=11.0),
        MagicMock(st_size=2, st_mtime=11.0),
    ]
    sensor = _get_sensor_with_associated_files(_mock_path(), mock_other_path)

    # when
    assert sensor._update_files_stat(now=0) == 0
    assert sensor._update_files_stat(now=7) == 7
    assert sensor._update_files_stat(now=8) == 0  # size changed
    assert sensor._update_files_stat(now=9) == 0  # mtime changed
    assert sensor._update_files_stat(now=12) == 3


def test_update_files_stat_file_vanished_after_listing() -> None:
    """Test _update_files_stat ignores a file that vanished between listing and stat."""
    mock_path = _mock_path()
    mock_vanished_path = MagicMock()
    mock_vanished_path.stat.side_effect = FileNotFoundError
    sensor = _get_sensor_with_associated_files(mock_path, mock_vanished_path)

    # when
    assert sensor._update_files_stat(now=0) == 0
    assert sensor._update_files_stat(now=3) == 3

    assert sensor._files_stat == {mock_path: (1, 10.0)}


@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
@patch("plugins.sensors.acquisition_monitor.get_raw_file_by_id")
def test_poke_file_dir_contents_change_file_is_removed(
    mock_get_raw_file_by_id: MagicMock,
    mock_update_raw_file: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test poke method correctly returns when dir contents change (file is removed)."""
    mock_monitor_wrapper = _setup_monitor_wrapper(
        mock_raw_file_wrapper_factory, _mock_path()
    )
    mock_monitor_wrapper.get_raw_files_on_instrument.side_effect = [
        {"some_file.raw", "some_file2.raw"},  # initial content (pre_execute)
        {"some_file.raw", "some_file2.raw"},  # first poke
        {"some_file.raw"},  # second poke
    ]

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    assert not sensor.poke({})
    assert not sensor.poke({})

    mock_update_raw_file.assert_called_once_with(
        mock_get_raw_file_by_id.return_value.id, new_status="monitoring_acquisition"
    )


@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
@patch("plugins.sensors.acquisition_monitor.get_raw_file_by_id")
def test_poke_file_dir_contents_change_main_file_does_not_exist(
    mock_get_raw_file_by_id: MagicMock,  # noqa: ARG001
    mock_update_raw_file: MagicMock,  # noqa: ARG001
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test poke method correctly returns when the main file does not exist."""
    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.main_file_path.return_value.exists.return_value = False

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    assert not sensor.poke({})
    assert not sensor._main_file_exists


@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.get_timestamp")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
@patch("plugins.sensors.acquisition_monitor.get_raw_file_by_id")
def test_poke_file_dir_contents_change_main_file_does_not_exist_for_too_long(
    mock_get_raw_file_by_id: MagicMock,  # noqa: ARG001
    mock_update_raw_file: MagicMock,  # noqa: ARG001
    mock_get_timestamp: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test poke method correctly returns when the main file does not show up for too long."""
    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.main_file_path.return_value.exists.return_value = False

    mock_get_timestamp.side_effect = [
        1,  # pre_execute (initial time stamp)
        2,  # first poke
        3,  # second poke
        SOFT_TIMEOUT_ON_MISSING_MAIN_FILE_M * 60 + 3,  # third poke => too long
    ]

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    assert not sensor.poke({})
    assert not sensor.poke({})
    assert sensor.poke({})

    assert not sensor._main_file_exists


@pytest.mark.parametrize(
    ("file_size", "expected_sensor_result"), [(100, True), (0, False)]
)
@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.get_timestamp")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
@patch("plugins.sensors.acquisition_monitor.get_raw_file_by_id")
def test_poke_file_dir_contents_dont_change_but_files_are_unchanged(  # noqa: PLR0913
    mock_get_raw_file_by_id: MagicMock,
    mock_update_raw_file: MagicMock,
    mock_get_timestamp: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
    *,
    file_size: int,
    expected_sensor_result: bool,
) -> None:
    """Test poke method correctly returns when dir contents do not change and files do not either (for cases file size = 0 and != 0)."""
    mock_path = _mock_path(size=file_size)
    mock_get_raw_file_by_id.return_value.original_name = "some_file.raw"
    mock_monitor_wrapper = _setup_monitor_wrapper(
        mock_raw_file_wrapper_factory, mock_path
    )
    mock_monitor_wrapper.get_raw_files_on_instrument.return_value = (
        set()
    )  # this stays constant

    mock_get_timestamp.side_effect = [
        1,  # pre_execute (initial time stamp)
        2,  # first poke
        3,  # second poke
        FILES_UNCHANGED_TIMEOUT_S + 2,  # third poke => unchanged for too long
    ]
    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    assert not sensor.poke({})
    assert not sensor.poke({})
    assert sensor.poke({}) == expected_sensor_result

    assert mock_path.stat.call_count == 3

    mock_update_raw_file.assert_called_once_with(
        mock_get_raw_file_by_id.return_value.id, new_status="monitoring_acquisition"
    )


@pytest.mark.parametrize(
    ("other_file_size", "expected_sensor_result"), [(10, True), (0, False)]
)
@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.get_timestamp")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
@patch("plugins.sensors.acquisition_monitor.get_raw_file_by_id")
def test_poke_file_dir_contents_dont_change_zero_size_main_file(  # noqa: PLR0913
    mock_get_raw_file_by_id: MagicMock,
    mock_update_raw_file: MagicMock,  # noqa: ARG001
    mock_get_timestamp: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
    *,
    other_file_size: int,
    expected_sensor_result: bool,
) -> None:
    """Test the zero-size guard considers the total size of all associated files, not only the main file."""
    mock_get_raw_file_by_id.return_value.original_name = "some_file.raw"
    mock_monitor_wrapper = _setup_monitor_wrapper(
        mock_raw_file_wrapper_factory,
        _mock_path(size=0),
        _mock_path(size=other_file_size),
    )
    mock_monitor_wrapper.get_raw_files_on_instrument.return_value = (
        set()
    )  # this stays constant

    mock_get_timestamp.side_effect = [
        0,  # pre_execute (initial time stamp)
        1,  # first poke
        FILES_UNCHANGED_TIMEOUT_S + 1,  # second poke => unchanged for too long
    ]
    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    assert not sensor.poke({})
    assert sensor.poke({}) == expected_sensor_result


@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.get_timestamp")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
@patch("plugins.sensors.acquisition_monitor.get_raw_file_by_id")
def test_poke_file_dir_contents_dont_change_all_associated_files_are_considered(
    mock_get_raw_file_by_id: MagicMock,
    mock_update_raw_file: MagicMock,  # noqa: ARG001
    mock_get_timestamp: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test poke method considers all associated files: acquisition is not done while a non-main file grows."""
    mock_main_path = _mock_path(size=100)
    mock_other_path = MagicMock()
    # grows between the first two pokes, then stays constant
    type(mock_other_path.stat.return_value).st_size = PropertyMock(
        side_effect=[10, 20, 20]
    )
    mock_get_raw_file_by_id.return_value.original_name = "some_file.raw"
    mock_monitor_wrapper = _setup_monitor_wrapper(
        mock_raw_file_wrapper_factory, mock_main_path, mock_other_path
    )
    mock_monitor_wrapper.get_raw_files_on_instrument.return_value = (
        set()
    )  # this stays constant

    mock_get_timestamp.side_effect = [
        1,  # pre_execute (initial time stamp)
        2,  # first poke
        3,  # second poke: other file grew
        FILES_UNCHANGED_TIMEOUT_S + 3,  # third poke => unchanged for too long
    ]
    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    assert not sensor.poke({})
    assert not sensor.poke({})
    assert sensor.poke({})

    mock_monitor_wrapper.get_associated_files.assert_called_with(
        mock_monitor_wrapper.instrument_path,
        mock_get_raw_file_by_id.return_value.original_name,
    )


@patch(
    "plugins.sensors.acquisition_monitor.AcquisitionMonitor._acquisition_finalized",
    return_value=False,
)
@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.get_timestamp")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
@patch("plugins.sensors.acquisition_monitor.get_raw_file_by_id")
def test_poke_file_dir_contents_dont_change_not_finalized(
    mock_get_raw_file_by_id: MagicMock,
    mock_update_raw_file: MagicMock,  # noqa: ARG001
    mock_get_timestamp: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
    mock_acquisition_finalized: MagicMock,  # noqa: ARG001
) -> None:
    """Test poke method waits for the finalization timeout if the raw file is not finalized."""
    mock_get_raw_file_by_id.return_value.original_name = "some_file.raw"
    mock_monitor_wrapper = _setup_monitor_wrapper(
        mock_raw_file_wrapper_factory, _mock_path()
    )
    mock_monitor_wrapper.get_raw_files_on_instrument.return_value = (
        set()
    )  # this stays constant

    mock_get_timestamp.side_effect = [
        0,  # pre_execute (initial time stamp)
        1,  # first poke
        FILES_UNCHANGED_TIMEOUT_S
        + 1,  # second poke => unchanged for too long, but not finalized
        FINALIZATION_TIMEOUT_S + 1,  # third poke => not finalized for too long
    ]
    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    assert not sensor.poke({})
    assert not sensor.poke({})
    assert sensor.poke({})


@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.get_timestamp")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
@patch("plugins.sensors.acquisition_monitor.get_raw_file_by_id")
def test_poke_file_dir_contents_dont_change_all_files_missing(
    mock_get_raw_file_by_id: MagicMock,
    mock_update_raw_file: MagicMock,  # noqa: ARG001
    mock_get_timestamp: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test poke method considers the acquisition done if no associated file exists for too long."""
    mock_get_raw_file_by_id.return_value.original_name = "some_file.raw"
    mock_monitor_wrapper = _setup_monitor_wrapper(mock_raw_file_wrapper_factory)
    mock_monitor_wrapper.get_raw_files_on_instrument.return_value = (
        set()
    )  # this stays constant

    mock_get_timestamp.side_effect = [
        0,  # pre_execute (initial time stamp)
        1,  # first poke
        FILES_UNCHANGED_TIMEOUT_S + 1,  # second poke => unchanged for too long
    ]
    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    assert not sensor.poke({})
    assert sensor.poke({})


def test_acquisition_finalized_non_sciex() -> None:
    """Test _acquisition_finalized is True for instruments without a finalization step."""
    sensor = get_sensor()
    sensor._raw_file_monitor_wrapper = MagicMock()

    assert sensor._acquisition_finalized()


@pytest.mark.parametrize(
    ("scan_file_size", "expected"),
    [
        (None, False),  # scan file missing
        (ZENO_ZT_SCAN_FILE_THRESHOLD_BYTES, False),
        (ZENO_ZT_SCAN_FILE_THRESHOLD_BYTES + 1, True),
    ],
)
def test_acquisition_finalized_sciex(
    tmp_path: Path, scan_file_size: int | None, *, expected: bool
) -> None:
    """Test _acquisition_finalized for Sciex depends on the .wiff.scan file exceeding the Zeno ZT threshold."""
    if scan_file_size is not None:
        (tmp_path / "sample.wiff.scan").write_bytes(b"x" * scan_file_size)
    sensor = get_sensor()
    sensor._raw_file_monitor_wrapper = MagicMock(spec=SciexRawFileMonitorWrapper)
    sensor._main_file_path = tmp_path / "sample.wiff"

    assert sensor._acquisition_finalized() == expected


oldest_age = 2
youngest_age = (OLD_FILE_THRESHOLD_H * 3600) + oldest_age + 1.0
intermediate_age = youngest_age // 2


@patch("plugins.sensors.acquisition_monitor.get_airflow_variable", return_value="True")
@patch("plugins.sensors.acquisition_monitor.get_file_ctime")
@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
@patch("plugins.sensors.acquisition_monitor.get_raw_file_by_id")
def test_poke_file_file_is_old(
    mock_get_raw_file_by_id: MagicMock,
    mock_update_raw_file: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
    mock_get_file_ctime: MagicMock,
    mock_get_airflow_variable: MagicMock,  # noqa: ARG001
) -> None:
    """Test poke method correctly returns when file is old and this mode is used."""
    mock_get_raw_file_by_id.return_value.original_name = "some_file.raw"
    mock_monitor_wrapper = _setup_monitor_wrapper(
        mock_raw_file_wrapper_factory, _mock_path()
    )
    mock_monitor_wrapper.get_raw_files_on_instrument.side_effect = [
        {"some_file.raw"},  # initial content (pre_execute)
        {"some_file.raw"},  # first poke
    ]
    mock_get_file_ctime.side_effect = [
        youngest_age,  # initial content (pre_execute) -> youngest file
        oldest_age,  # first poke
    ]

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})

    # when
    assert sensor.poke({})

    mock_update_raw_file.assert_called_once_with(
        mock_get_raw_file_by_id.return_value.id, new_status="monitoring_acquisition"
    )


@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.put_xcom")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
@patch("plugins.sensors.acquisition_monitor.get_raw_file_by_id")
def test_post_execute_ok(
    mock_get_raw_file_by_id: MagicMock,
    mock_update_raw_file: MagicMock,
    mock_put_xcom: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test post_execute correctly works if no acquisition errors."""
    mock_monitor_wrapper = _setup_monitor_wrapper(
        mock_raw_file_wrapper_factory, _mock_path()
    )
    mock_monitor_wrapper.get_raw_files_on_instrument.side_effect = [
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
            call(
                mock_get_raw_file_by_id.return_value.id,
                new_status="monitoring_acquisition",
            ),
            call(mock_get_raw_file_by_id.return_value.id, new_status="monitoring_done"),
        ]
    )


@patch("plugins.sensors.acquisition_monitor.RawFileWrapperFactory")
@patch("plugins.sensors.acquisition_monitor.put_xcom")
@patch("plugins.sensors.acquisition_monitor.update_raw_file")
@patch("plugins.sensors.acquisition_monitor.get_raw_file_by_id")
def test_post_execute_acquisition_errors(
    mock_get_raw_file_by_id: MagicMock,
    mock_update_raw_file: MagicMock,
    mock_put_xcom: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test post_execute correctly reports a missing main file."""
    mock_path = MagicMock()
    mock_path.exists.return_value = False
    mock_monitor_wrapper = _setup_monitor_wrapper(
        mock_raw_file_wrapper_factory, mock_path
    )
    mock_monitor_wrapper.get_raw_files_on_instrument.return_value = {"some_file.raw"}
    mock_monitor_wrapper.main_file_name = "analysis.tdf_bin"

    sensor = get_sensor()
    sensor.pre_execute({DagContext.PARAMS: {DagParams.RAW_FILE_ID: "some_file.raw"}})
    sensor._first_poke_timestamp = 0

    ti = MagicMock()

    # when
    sensor.poke({})
    sensor.post_execute({"ti": ti}, result=True)

    mock_put_xcom.assert_called_once_with(
        ti,
        "acquisition_monitor_errors",
        ["Main file was not created: analysis.tdf_bin"],
    )

    mock_update_raw_file.assert_has_calls(
        [
            call(
                mock_get_raw_file_by_id.return_value.id,
                new_status="monitoring_acquisition",
            ),
            call(mock_get_raw_file_by_id.return_value.id, new_status="monitoring_done"),
        ]
    )


def test_get_youngest_file_age_directory_empty() -> None:
    """Test _get_youngest_file_age returns 0 when directory is empty."""
    assert AcquisitionMonitor._get_youngest_file_age(Path("file_to_check"), set()) == 0


@patch("plugins.sensors.acquisition_monitor.get_file_ctime")
def test_get_youngest_file_age(mock_get_file_ctime: MagicMock) -> None:
    """Test _get_youngest_file_age returns correctly."""
    mock_get_file_ctime.side_effect = [oldest_age, intermediate_age, youngest_age]
    assert (
        AcquisitionMonitor._get_youngest_file_age(
            Path("file_to_check"), {"file1", "file2", "file3"}
        )
        == youngest_age
    )


@patch("plugins.sensors.acquisition_monitor.get_file_ctime")
def test_get_youngest_file_age_file_vanished(mock_get_file_ctime: MagicMock) -> None:
    """Test _get_youngest_file_age skips a file that vanished during the scan."""
    mock_get_file_ctime.side_effect = [oldest_age, FileNotFoundError, intermediate_age]
    assert (
        AcquisitionMonitor._get_youngest_file_age(
            Path("file_to_check"), {"file1", "file2", "file3"}
        )
        == intermediate_age
    )


@patch("plugins.sensors.acquisition_monitor.get_file_ctime")
def test_get_youngest_file_age_all_files_vanished(
    mock_get_file_ctime: MagicMock,
) -> None:
    """Test _get_youngest_file_age returns 0 if all files vanished during the scan."""
    mock_get_file_ctime.side_effect = FileNotFoundError
    assert (
        AcquisitionMonitor._get_youngest_file_age(Path("file_to_check"), {"file1"}) == 0
    )


@pytest.mark.parametrize(
    ("age", "expected"),
    [
        (youngest_age, False),
        (intermediate_age, False),
        (oldest_age, True),
    ],
)
@patch("plugins.sensors.acquisition_monitor.get_file_ctime")
def test_is_older_than_threshold(
    mock_get_file_ctime: MagicMock,
    age: float,
    *,
    expected: bool,
) -> None:
    """Test _is_older_than_threshold returns correctly for several cases."""
    mock_get_file_ctime.return_value = age

    # when
    assert (
        AcquisitionMonitor._is_older_than_threshold(Path("file_name"), youngest_age)
        == expected
    )
