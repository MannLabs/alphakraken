"""Tests for the db.interface module."""

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytz
from db.models import RawFileStatus

from shared.db.interface import (
    add_metrics_to_raw_file,
    add_new_project_to_db,
    add_new_raw_file_to_db,
    add_new_settings_to_db,
    get_raw_file_names_from_db,
    update_raw_file_status,
)


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.RawFile")
def test_add_new_raw_file_to_db_creates_new_file_when_file_does_not_exist(
    mock_raw_file: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that add_new_raw_file_to_db creates a new file when the file does not exist in the database."""
    # given
    mock_raw_file.return_value.save.side_effect = None
    # when
    add_new_raw_file_to_db(
        "test_file.raw", instrument_id="instrument1", size=42.0, creation_ts=43.0
    )

    # then
    mock_raw_file.assert_called_once_with(
        name="test_file.raw",
        status=RawFileStatus.NEW,
        size=42.0,
        instrument_id="instrument1",
        created_at=datetime(1970, 1, 1, 0, 0, 43, tzinfo=pytz.utc),
    )
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.RawFile")
def test_get_raw_file_names_from_db_returns_expected_names_when_files_exist(
    mock_raw_file: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that get_raw_file_names_from_db returns the expected names when the files exist in the database."""
    # given
    mock1 = MagicMock()
    mock1.name = "file1"
    mock2 = MagicMock()
    mock2.name = "file2"
    mock_raw_file.objects.filter.return_value = [mock1, mock2]

    # when
    result = get_raw_file_names_from_db(["file1", "file2"])

    # then
    assert result == ["file1", "file2"]
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.RawFile")
def test_get_raw_file_names_from_db_returns_empty_list_when_no_files_exist(
    mock_raw_file: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that get_raw_file_names_from_db returns an empty list when no files exist in the database."""
    # given
    mock_raw_file.objects.filter.return_value = []
    # when
    result = get_raw_file_names_from_db(["file1", "file2"])
    # then
    assert result == []
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.RawFile")
def test_get_raw_file_names_from_db_returns_only_existing_files_when_some_files_do_not_exist(
    mock_raw_file: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that get_raw_file_names_from_db returns only the names of the files that exist in the database."""
    # given
    mock1 = MagicMock()
    mock1.name = "file1"
    mock_raw_file.objects.filter.return_value = [mock1]
    # when
    result = get_raw_file_names_from_db(["file1", "file2"])
    # then
    assert result == ["file1"]
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.RawFile")
def test_update_raw_file_status(
    mock_raw_file: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that update_raw_file_status updates the status of the raw file."""
    # given
    mock_raw_file_from_db = MagicMock()
    mock_raw_file.objects.with_id.return_value = mock_raw_file_from_db

    # when
    update_raw_file_status("test_file", RawFileStatus.PROCESSED)

    # then
    mock_raw_file_from_db.update.assert_called_once_with(status=RawFileStatus.PROCESSED)
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
    add_metrics_to_raw_file("test_file", {"metric1": 1, "metric2": 2})

    # then
    mock_metrics.return_value.save.assert_called_once()
    mock_connect_db.assert_called_once()
    mock_raw_file.objects.get.assert_called_once_with(name="test_file")
    mock_metrics.assert_called_once_with(
        raw_file=mock_raw_file_from_db, metric1=1, metric2=2
    )


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.Project")
def test_add_new_project_to_db(
    mock_project: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that add_new_project_to_db adds a new project to the database."""
    # given
    mock_project.return_value.save.side_effect = None

    # when
    add_new_project_to_db(
        project_id="P1234", name="new project", description="some project description"
    )

    # then
    mock_project.assert_called_once_with(
        id="P1234", name="new project", description="some project description"
    )
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.Settings")
@patch("shared.db.interface.Project")
def test_add_new_settings_to_db(
    mock_project: MagicMock, mock_settings: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that add_new_settings_to_db adds new settings to the database."""
    # given
    mock_project_from_db = MagicMock()
    mock_project.objects.get.return_value = mock_project_from_db

    mock_settings.objects.return_value.first.return_value = None

    # when
    add_new_settings_to_db(
        project_id="P1234",
        name="new settings",
        fasta_file_name="fasta_file",
        speclib_file_name="speclib_file",
        config_file_name="config_file",
        software="software",
    )

    # then
    mock_settings.assert_called_once_with(
        project=mock_project_from_db,
        name="new settings",
        fasta_file_name="fasta_file",
        speclib_file_name="speclib_file",
        config_file_name="config_file",
        software="software",
    )
    mock_connect_db.assert_called_once()
