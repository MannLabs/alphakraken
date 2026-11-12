# type: ignore[invalid-argument-type]

"""Tests for the db.interface module."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
import pytz
from pymongo import UpdateOne

from shared.db.interface import (
    add_metrics_to_raw_file,
    add_raw_file,
    get_purge_candidates_by_age,
    get_raw_file_by_id,
    get_raw_files_by_names,
    get_slurm_job,
    update_raw_file,
    update_slurm_jobs,
)
from shared.domain.raw_files import InstrumentFileStatus, RawFileStatus


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
        instrument_file_status=InstrumentFileStatus.INITIAL,
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
        instrument_file_status=InstrumentFileStatus.INITIAL,
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
def test_get_raw_files_by_names_filters_by_exact_names_if_case_sensitive(
    mock_raw_file: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that get_raw_files_by_names queries the exact names if case sensitivity is requested."""
    # when
    get_raw_files_by_names(["file1.raw", "file2.raw"], case_sensitive=True)

    # then
    mock_raw_file.objects.filter.assert_called_once_with(
        original_name__in=["file1.raw", "file2.raw"]
    )
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.RawFile")
def test_get_raw_files_by_names_filters_case_insensitively(
    mock_raw_file: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that get_raw_files_by_names matches names regardless of case."""
    # when
    get_raw_files_by_names(["file1.raw"])

    # then
    patterns = mock_raw_file.objects.filter.call_args.kwargs["original_name__in"]
    assert [p.pattern for p in patterns] == [r"^file1\.raw$"]
    assert patterns[0].match("FILE1.raw")
    assert not patterns[0].match("xfile1.rawx")
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
def test_get_purge_candidates_by_age(
    mock_datetime: MagicMock, mock_raw_file: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test get raw file ids from DB returns correct value."""
    file1 = MagicMock()
    filter_mock = MagicMock()
    filter_mock.order_by.return_value = [file1]
    mock_raw_file.objects.filter.return_value = filter_mock

    mock_datetime.now.return_value = datetime(2022, 7, 5, 4, 16, 0, 0, tzinfo=pytz.UTC)

    # when
    result = get_purge_candidates_by_age("instrument1", min_age_in_days=30)

    assert result == [file1]

    mock_connect_db.assert_called_once()
    mock_raw_file.objects.filter.assert_called_once_with(
        instrument_id="instrument1",
        created_at__lt=mock_datetime.now.return_value - timedelta(days=30),
        instrument_file_status__nin=[
            InstrumentFileStatus.PURGED,
            InstrumentFileStatus.RENAMED,
            InstrumentFileStatus.DISAPPEARED,
        ],
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
        settings_name="test_settings",
        settings_version=1,
        relative_output_path="P1/out_test_file/alphadia",
        output_display_base_path="/fs/output",
    )

    # then
    mock_metrics.return_value.save.assert_called_once()
    mock_connect_db.assert_called_once()
    mock_raw_file.objects.get.assert_called_once_with(id="test_file")
    mock_metrics.assert_called_once_with(
        raw_file=mock_raw_file_from_db,
        type="alphadia",
        settings_name="test_settings",
        settings_version=1,
        relative_output_path="P1/out_test_file/alphadia",
        output_display_base_path="/fs/output",
        metric1=1,
        metric2=2,
    )


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.SlurmJob")
@patch("shared.db.interface.datetime")
def test_update_slurm_jobs_upserts_queued_and_marks_left_jobs(
    mock_datetime: MagicMock, mock_slurm_job: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that queued jobs are upserted and the other stored jobs are marked as having left the queue."""
    now = mock_datetime.now.return_value

    # when
    update_slurm_jobs("cluster1", {"123": "RUNNING"})

    # then
    mock_slurm_job._get_collection.return_value.bulk_write.assert_called_once_with(
        [
            UpdateOne(
                {"_id": "cluster1/123"},
                {
                    "$set": {
                        "cluster_id": "cluster1",
                        "state": "RUNNING",
                        "updated_at_": now,
                    }
                },
                upsert=True,
            )
        ],
        ordered=False,
    )
    mock_slurm_job.objects.assert_called_once_with(
        cluster_id="cluster1", state__ne="LEFT_QUEUE", id__nin=["cluster1/123"]
    )
    mock_slurm_job.objects.return_value.update.assert_called_once_with(
        set__state="LEFT_QUEUE", set__updated_at_=now
    )
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.SlurmJob")
def test_update_slurm_jobs_empty_queue_marks_all_left(
    mock_slurm_job: MagicMock,
    mock_connect_db: MagicMock,  # noqa: ARG001
) -> None:
    """Test that with an empty queue, nothing is upserted and all stored jobs are marked as having left the queue."""
    # when
    update_slurm_jobs("cluster1", {})

    # then
    mock_slurm_job._get_collection.return_value.bulk_write.assert_not_called()
    mock_slurm_job.objects.assert_called_once_with(
        cluster_id="cluster1", state__ne="LEFT_QUEUE", id__nin=[]
    )


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.SlurmJob")
def test_get_slurm_job(mock_slurm_job: MagicMock, mock_connect_db: MagicMock) -> None:
    """Test that get_slurm_job queries by the id combining cluster and job id."""
    # when
    result = get_slurm_job("cluster1", "123")

    # then
    mock_slurm_job.objects.assert_called_once_with(id="cluster1/123")
    assert result == mock_slurm_job.objects.return_value.first.return_value
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.RawFile")
def test_update_raw_file_with_instrument_file_status(
    mock_raw_file: MagicMock,
    mock_connect_db: MagicMock,  # noqa: ARG001
) -> None:
    """Test that update_raw_file can update instrument_file_status field."""
    # given
    mock_raw_file_instance = MagicMock()
    mock_raw_file_instance.status = "copying_done"
    mock_raw_file_instance.status_details = "details"
    mock_raw_file.objects.with_id.return_value = mock_raw_file_instance

    # when
    update_raw_file("test_id", instrument_file_status=InstrumentFileStatus.MOVED)

    # then
    mock_raw_file.objects.with_id.assert_called_once_with("test_id")
    mock_raw_file_instance.update.assert_called_once()
    update_call_args = mock_raw_file_instance.update.call_args[1]
    assert "instrument_file_status" in update_call_args
    assert update_call_args["instrument_file_status"] == InstrumentFileStatus.MOVED
