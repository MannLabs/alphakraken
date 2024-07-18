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
    get_all_project_ids,
    get_raw_file_names_from_db,
    update_kraken_status,
    update_raw_file,
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
        "test_file.raw",
        collision_flag="123---",
        project_id="PID1",
        instrument_id="instrument1",
        status=RawFileStatus.QUEUED_FOR_MONITORING,
        creation_ts=43.0,
    )

    # then
    mock_raw_file.assert_called_once_with(
        name="123---test_file.raw",
        original_name="test_file.raw",
        collision_flag="123---",
        project_id="PID1",
        instrument_id="instrument1",
        status=RawFileStatus.QUEUED_FOR_MONITORING,
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
    file1 = MagicMock()
    file2 = MagicMock()
    mock_raw_file.objects.filter.return_value = [file1, file2]

    # when
    result = get_raw_file_names_from_db(["file1", "file2"])

    # then
    assert result == [file1, file2]
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
    file1 = MagicMock()
    mock_raw_file.objects.filter.return_value = [file1]
    # when
    result = get_raw_file_names_from_db(["file1", "file2"])
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
    update_raw_file("test_file", new_status=RawFileStatus.DONE, size=123)

    # then
    mock_raw_file_from_db.update.assert_called_once_with(
        status=RawFileStatus.DONE,
        updated_at_=mock_datetime.now.return_value,
        status_details=None,
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
    )

    # then
    mock_krakenstatus.assert_called_once_with(
        instrument_id="instrument1",
        status="error",
        updated_at_=mock_datetime.now.return_value,
        status_details="some details",
        last_error_occurred_at=mock_datetime.now.return_value,
    )
    mock_connect_db.assert_called_once()


# def update_kraken_status(
#     instrument_id: str, *, status: str, status_details: str
# ) -> None:
#     """Update the status of a instrument connected to kraken."""
#     logging.info(f"Updating DB: {instrument_id=} to {status=} with {status_details=}")
#     connect_db()
#     error_args = (
#         {"last_error_occurred_at": datetime.now(tz=pytz.utc)}
#         if status == KrakenStatusValues.ERROR
#         else {}
#     )
#
#     KrakenStatus(
#         instrument_id=instrument_id,
#         status=status,
#         updated_at_=datetime.now(tz=pytz.utc),
#         status_details=status_details,
#         **error_args,
#     ).save()
