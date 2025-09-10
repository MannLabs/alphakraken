"""Tests for the db.interface module."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
import pytz

from shared.db.interface import (
    add_metrics_to_raw_file,
    add_project,
    add_raw_file,
    add_settings,
    get_all_project_ids,
    get_raw_file_by_id,
    get_raw_files_by_age,
    get_raw_files_by_names,
    update_kraken_status,
    update_raw_file,
)
from shared.db.models import RawFileStatus


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.RawFile")
@pytest.mark.parametrize(
    ("collision_flag", "collision_string"), [(None, ""), ("123-", "123-")]
)
def test_add_raw_file_creates_new_file_when_file_does_not_exist_with_collision_flag(
    mock_raw_file: MagicMock,
    mock_connect_db: MagicMock,
    collision_flag: str | None,
    collision_string: str,
) -> None:
    """Test that add_raw_file creates a new file when the file does not exist in the database."""
    # given
    mock_raw_file.return_value.save.side_effect = None
    # when
    add_raw_file(
        "test_file.raw",
        collision_flag=collision_flag,
        project_id="PID1",
        instrument_id="instrument1",
        status=RawFileStatus.QUEUED_FOR_MONITORING,
        creation_ts=43.0,
    )

    # then
    mock_raw_file.assert_called_once_with(
        id=f"{collision_string}test_file.raw",
        original_name="test_file.raw",
        collision_flag=collision_flag,
        project_id="PID1",
        instrument_id="instrument1",
        status=RawFileStatus.QUEUED_FOR_MONITORING,
        created_at=datetime(1970, 1, 1, 0, 0, 43, tzinfo=pytz.utc),
    )
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.RawFile")
def test_get_raw_files_by_names_returns_expected_names_when_files_exist(
    mock_raw_file: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that get_raw_files_by_names returns the expected names when the files exist in the database."""
    # given
    file1 = MagicMock()
    file2 = MagicMock()
    mock_raw_file.objects.filter.return_value = [file1, file2]

    # when
    result = get_raw_files_by_names(["file1", "file2"])

    # then
    assert result == [file1, file2]
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.RawFile")
def test_get_raw_file_by_id(
    mock_raw_file: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test get raw file with the given id from DB returns correct value."""
    file1 = MagicMock()
    mock_raw_file.objects.return_value.first.return_value = file1

    result = get_raw_file_by_id("file1")

    # then
    assert result == file1

    mock_connect_db.assert_called_once()
    mock_raw_file.objects.assert_called_once_with(id="file1")


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.RawFile")
@patch("shared.db.interface.datetime")
def test_get_raw_files_by_age(
    mock_datetime: MagicMock, mock_raw_file: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test get raw file ids from DB returns correct value."""
    file1 = MagicMock()
    filter_mock = MagicMock()
    filter_mock.order_by.return_value = [file1]
    mock_raw_file.objects.filter.return_value = filter_mock

    mock_datetime.now.return_value = datetime(2022, 7, 5, 4, 16, 0, 0, tzinfo=pytz.UTC)

    # when
    result = get_raw_files_by_age("instrument1", min_age_in_days=30, max_age_in_days=60)

    assert result == [file1]

    mock_connect_db.assert_called_once()
    mock_raw_file.objects.filter.assert_called_once_with(
        instrument_id="instrument1",
        created_at__lt=mock_datetime.now.return_value - timedelta(days=30),
        created_at__gte=mock_datetime.now.return_value - timedelta(days=60),
    )


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.RawFile")
def test_get_raw_files_by_names_returns_empty_list_when_no_files_exist(
    mock_raw_file: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that get_raw_files_by_names returns an empty list when no files exist in the database."""
    # given
    mock_raw_file.objects.filter.return_value = []
    # when
    result = get_raw_files_by_names(["file1", "file2"])
    # then
    assert result == []
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.RawFile")
def test_get_raw_files_by_names_returns_only_existing_files_when_some_files_do_not_exist(
    mock_raw_file: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that get_raw_files_by_names returns only the names of the files that exist in the database."""
    # given
    file1 = MagicMock()
    mock_raw_file.objects.filter.return_value = [file1]
    # when
    result = get_raw_files_by_names(["file1", "file2"])
    # then
    assert result == [file1]
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.RawFile")
@patch("shared.db.interface.datetime")
def test_update_raw_file(
    mock_datetime: MagicMock, mock_raw_file: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that update_raw_file updates the status and size of the raw file."""
    # given
    mock_raw_file_from_db = MagicMock()
    mock_raw_file.objects.with_id.return_value = mock_raw_file_from_db

    # when
    update_raw_file(
        "test_file", new_status=RawFileStatus.DONE, status_details=None, size=123
    )

    # then
    mock_raw_file_from_db.update.assert_called_once_with(
        status_details=None,
        status=RawFileStatus.DONE,
        updated_at_=mock_datetime.now.return_value,
        size=123,
    )
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.RawFile")
@patch("shared.db.interface.Metrics")
def test_add_metrics_to_raw_file_happy_path(
    mock_metrics: MagicMock, mock_raw_file: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that add_metrics_to_raw_file saves the metrics to the database."""
    # given
    mock_raw_file_from_db = MagicMock()
    mock_raw_file.objects.get.return_value = mock_raw_file_from_db

    # when
    add_metrics_to_raw_file(
        "test_file",
        metrics_type="alphadia",
        metrics={"metric1": 1, "metric2": 2},
        settings_version=1,
    )

    # then
    mock_metrics.return_value.save.assert_called_once()
    mock_connect_db.assert_called_once()
    mock_raw_file.objects.get.assert_called_once_with(id="test_file")
    mock_metrics.assert_called_once_with(
        raw_file=mock_raw_file_from_db,
        type="alphadia",
        metric1=1,
        metric2=2,
        settings_version=1,
    )


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.Project")
def test_add_project(mock_project: MagicMock, mock_connect_db: MagicMock) -> None:
    """Test that add_project adds a new project to the database."""
    # given
    mock_project.return_value.save.side_effect = None

    # when
    add_project(
        project_id="P1234", name="new project", description="some project description"
    )

    # then
    mock_project.assert_called_once_with(
        id="P1234", name="new project", description="some project description"
    )
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.Project")
def test_get_all_project_ids(
    mock_project: MagicMock, mock_connect_db: MagicMock
) -> None:
    """get_all_project_ids returns all project ids."""
    # given
    mock_project.objects.all.return_value = [
        MagicMock(id="P1234"),
        MagicMock(id="P1235"),
    ]

    # when
    result = get_all_project_ids()

    # then
    assert result == ["P1234", "P1235"]

    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.Settings")
@patch("shared.db.interface.Project")
def test_add_settings_first(
    mock_project: MagicMock, mock_settings: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that add_settings adds new settings to the database (first setting)."""
    # given
    mock_project_from_db = MagicMock()
    mock_project.objects.get.return_value = mock_project_from_db

    mock_settings.objects.return_value.first.return_value = None

    # when
    add_settings(
        project_id="P1234",
        name="new settings",
        fasta_file_name="fasta_file",
        speclib_file_name="speclib_file",
        config_file_name="config_file",
        config_params="config_params",
        software_type="software_type",
        software="software",
    )

    # then
    mock_settings.assert_called_once_with(
        project=mock_project_from_db,
        name="new settings",
        fasta_file_name="fasta_file",
        speclib_file_name="speclib_file",
        config_file_name="config_file",
        config_params="config_params",
        software_type="software_type",
        software="software",
        version=1,
    )
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.Settings")
@patch("shared.db.interface.Project")
def test_add_settings_not_first(
    mock_project: MagicMock, mock_settings: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that add_settings adds new settings to the database (not the first setting)."""
    # given
    mock_project_from_db = MagicMock()
    mock_project.objects.get.return_value = mock_project_from_db

    mock_settings.objects.return_value.first.return_value = MagicMock()
    mock_settings.objects.return_value.all.return_value.count.return_value = 41

    # when
    add_settings(
        project_id="P1234",
        name="new settings",
        fasta_file_name="fasta_file",
        speclib_file_name="speclib_file",
        config_file_name="config_file",
        config_params="config_params",
        software_type="software_type",
        software="software",
    )

    # then
    mock_settings.assert_called_once_with(
        project=mock_project_from_db,
        name="new settings",
        fasta_file_name="fasta_file",
        speclib_file_name="speclib_file",
        config_file_name="config_file",
        config_params="config_params",
        software_type="software_type",
        software="software",
        version=42,
    )
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.KrakenStatus")
@patch("shared.db.interface.datetime")
def test_update_kraken_status(
    mock_datetime: MagicMock, mock_krakenstatus: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that update_kraken_status updates the status correctly."""
    # when
    update_kraken_status(
        instrument_id="instrument1",
        status="error",
        status_details="some details",
        free_space_gb=123,
    )

    # then
    mock_krakenstatus.assert_called_once_with(
        instrument_id="instrument1",
        status="error",
        updated_at_=mock_datetime.now.return_value,
        status_details="some details",
        free_space_gb=123,
    )
    mock_connect_db.assert_called_once()
