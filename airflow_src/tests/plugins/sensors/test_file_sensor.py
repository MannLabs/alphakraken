"""Unit tests for the file sensor plugin."""

from datetime import datetime

# ruff: noqa: SLF001 # Private member accessed
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytz
from plugins.sensors.file_sensor import (
    FileCreationSensor,
    _check_health,
    _check_path_health,
)

from shared.db.models import KrakenStatusValues


def get_sensor() -> FileCreationSensor:
    """Get an instance of the sensor."""
    with patch(
        "plugins.sensors.file_sensor.get_internal_instrument_data_path"
    ) as mock_get:
        mock_get.return_value = Path("/opt/airflow/acquisition_pcs/apc_tims_1")
        return FileCreationSensor(
            task_id="some_task_id", instrument_id="some_instrument_id"
        )


@patch("plugins.sensors.file_sensor.RawFileWrapperFactory")
@patch("plugins.sensors.file_sensor._check_health")
def test_poke_file_not_created(
    mock_check_health: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test poke method when file is not created."""
    # given
    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.get_raw_files_on_instrument.side_effect = [
        {"some_file.raw", "some_file2.raw"},  # initial content (pre_execute)
        {"some_file.raw", "some_file2.raw"},  # first poke
    ]

    ti = MagicMock()

    # when
    sensor = get_sensor()
    sensor.pre_execute({})
    result = sensor.poke({"task_instance": ti})

    # then
    assert not result
    mock_check_health.assert_called_once_with("some_instrument_id")


@patch("plugins.sensors.file_sensor.RawFileWrapperFactory")
@patch("plugins.sensors.file_sensor._check_health")
def test_poke_file_created(
    mock_check_health: MagicMock,
    mock_raw_file_wrapper_factory: MagicMock,
) -> None:
    """Test poke method when file is created."""
    # given
    mock_raw_file_wrapper_factory.create_monitor_wrapper.return_value.get_raw_files_on_instrument.side_effect = [
        {"some_file.raw"},  # initial content (pre_execute)
        {"some_file.raw", "some_file2.raw"},  # first poke
    ]

    ti = MagicMock()

    # when
    sensor = get_sensor()
    sensor.pre_execute({})
    result = sensor.poke({"task_instance": ti})

    # then
    assert result
    mock_check_health.assert_called_once_with("some_instrument_id")


@patch("plugins.sensors.file_sensor.RawFileWrapperFactory")
@patch("plugins.sensors.file_sensor._check_health")
@patch("plugins.sensors.file_sensor.datetime")
def test_file_creation_sensor_timeout(
    mock_datetime: MagicMock,
    mock_check_health: MagicMock,  # noqa: ARG001
    mock_raw_file_wrapper_factory: MagicMock,  # noqa: ARG001
) -> None:
    """Test that the sensor times out after the specified timeout period."""
    mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0, tzinfo=pytz.utc)
    sensor = get_sensor()
    sensor._start_time = datetime(2023, 1, 1, 6, 0, 0, tzinfo=pytz.utc)

    # Simulate the passage of time to trigger the timeout
    mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 1, 0, tzinfo=pytz.utc)

    assert sensor.poke({}) is True


@patch("plugins.sensors.file_sensor.update_kraken_status")
@patch("plugins.sensors.file_sensor.get_internal_instrument_data_path")
@patch("plugins.sensors.file_sensor.get_internal_backup_path_for_instrument")
@patch("plugins.sensors.file_sensor.get_internal_output_path")
@patch("plugins.sensors.file_sensor.get_disk_usage")
def test_check_health_when_all_paths_exist(
    mock_get_disk_usage: MagicMock,
    mock_get_output_path: MagicMock,
    mock_get_backup_path: MagicMock,
    mock_get_data_path: MagicMock,
    mock_update_status: MagicMock,
) -> None:
    """Test that the health check passes when both paths exist."""
    mock_path = MagicMock()
    mock_path.__truediv__.return_value.stat.side_effect = FileNotFoundError  # sentinel
    mock_path.rglob.side_effect = [["file1"], ["file1"], ["file1"]]
    mock_get_data_path.return_value = mock_path
    mock_get_backup_path.return_value = mock_path
    mock_get_output_path.return_value = mock_path
    mock_get_disk_usage.return_value = (123, 456, 789)

    # when
    _check_health("instrument_id")

    # Check that three calls were made: instrument, backup filesystem, output filesystem
    assert mock_update_status.call_count == 3

    # Check instrument status call
    mock_update_status.assert_any_call(
        "instrument_id",
        status=KrakenStatusValues.OK,
        status_details="",
        free_space_gb=789,
    )

    mock_update_status.assert_any_call(
        "backup",
        status=KrakenStatusValues.OK,
        status_details="",
        free_space_gb=789,
        entity_type="file_system",
    )

    mock_update_status.assert_any_call(
        "output",
        status=KrakenStatusValues.OK,
        status_details="",
        free_space_gb=789,
        entity_type="file_system",
    )


@patch("plugins.sensors.file_sensor.update_kraken_status")
@patch("plugins.sensors.file_sensor.get_internal_instrument_data_path")
@patch("plugins.sensors.file_sensor.get_internal_backup_path_for_instrument")
@patch("plugins.sensors.file_sensor.get_internal_output_path")
@patch("plugins.sensors.file_sensor.get_disk_usage")
def test_check_health_when_no_paths_exist(
    mock_get_disk_usage: MagicMock,
    mock_get_output_path: MagicMock,
    mock_get_backup_path: MagicMock,
    mock_get_data_path: MagicMock,
    mock_update_status: MagicMock,
) -> None:
    """Test that the health check fails when both paths do not exist."""
    mock_path = MagicMock()
    mock_path.stat.side_effect = FileNotFoundError
    mock_get_data_path.return_value = mock_path
    mock_get_backup_path.return_value = mock_path
    mock_get_output_path.return_value = mock_path
    mock_get_disk_usage.return_value = (123, 456, 789)

    # when
    _check_health("instrument_id")

    # Check that three calls were made: instrument, backup filesystem, output filesystem
    assert mock_update_status.call_count == 3

    # Check instrument status call
    mock_update_status.assert_any_call(
        "instrument_id",
        status="error",
        status_details="data path not healthy (exists=False mounted=None has_files=None); backup path not healthy (exists=False mounted=None has_files=None); output path not healthy (exists=False mounted=None has_files=None)",
        free_space_gb=789,
    )

    mock_update_status.assert_any_call(
        "backup",
        status="ok",
        status_details="",
        free_space_gb=789,
        entity_type="file_system",
    )

    mock_update_status.assert_any_call(
        "output",
        status="ok",
        status_details="",
        free_space_gb=789,
        entity_type="file_system",
    )


def test_check_path_health_when_healthy(tmp_path: Path) -> None:
    """Test that a non-empty path without sentinel adds no status details."""
    (tmp_path / "file1").touch()
    status_details: list[str] = []

    _check_path_health(tmp_path, "data", status_details)

    assert status_details == []


def test_check_path_health_when_not_mounted(tmp_path: Path) -> None:
    """Test that a visible sentinel file is reported as not mounted."""
    (tmp_path / "alphakraken_local_dir_sentinel").touch()
    status_details: list[str] = []

    _check_path_health(tmp_path, "data", status_details)

    assert status_details == [
        "data path not healthy (exists=True mounted=False has_files=None)"
    ]


def test_check_path_health_when_empty(tmp_path: Path) -> None:
    """Test that an empty path is reported."""
    status_details: list[str] = []

    _check_path_health(tmp_path, "data", status_details)

    assert status_details == [
        "data path not healthy (exists=True mounted=True has_files=False)"
    ]


def test_check_path_health_when_not_accessible() -> None:
    """Test that an OSError is reported instead of raised."""
    mock_path = MagicMock()
    mock_path.stat.side_effect = OSError(112, "Host is down")
    status_details: list[str] = []

    _check_path_health(mock_path, "data", status_details)

    assert status_details == ["data path not accessible ([Errno 112] Host is down)"]
    mock_path.rglob.assert_not_called()


def test_check_path_health_when_sentinel_lookup_not_accessible() -> None:
    """Test that an OSError on the sentinel lookup is reported, not read as mounted."""
    mock_path = MagicMock()
    mock_path.__truediv__.return_value.stat.side_effect = OSError(112, "Host is down")
    status_details: list[str] = []

    _check_path_health(mock_path, "data", status_details)

    assert status_details == ["data path not accessible ([Errno 112] Host is down)"]
