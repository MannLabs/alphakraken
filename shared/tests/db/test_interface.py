# type: ignore[invalid-argument-type]

"""Tests for the db.interface module."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
import pytz

from shared.db.interface import (
    add_metrics_to_raw_file,
    add_project,
    add_raw_file,
    assign_settings_to_project,
    augment_raw_files_with_metrics,
    create_settings,
    get_all_project_ids,
    get_all_settings,
    get_raw_file_by_id,
    get_raw_files_by_age,
    get_raw_files_by_instrument_file_status,
    get_raw_files_by_names,
    update_kraken_status,
    update_raw_file,
)
from shared.db.models import InstrumentFileStatus, RawFileStatus
from shared.keys import MetricsTypes


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
def test_create_settings_first_version(
    mock_settings: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that create_settings creates first version of settings."""
    mock_settings.objects.return_value.order_by.return_value.first.return_value = None

    create_settings(
        name="plasma_settings",
        description="Fast plasma settings",
        fasta_file_name="human.fasta",
        speclib_file_name="plasma.speclib",
        config_file_name="fast.yaml",
        config_params=None,
        software_type="alphadia",
        software="alphadia-1.10.0",
    )

    mock_settings.assert_called_once()
    call_kwargs = mock_settings.call_args.kwargs
    assert call_kwargs["name"] == "plasma_settings"
    assert call_kwargs["version"] == 1
    assert call_kwargs["description"] == "Fast plasma settings"
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.Settings")
def test_create_settings_auto_increment_version(
    mock_settings: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that create_settings auto-increments version."""
    existing_setting = MagicMock()
    existing_setting.version = 3
    mock_settings.objects.return_value.order_by.return_value.first.return_value = (
        existing_setting
    )

    create_settings(
        name="plasma_settings",
        description=None,
        fasta_file_name="human.fasta",
        speclib_file_name="plasma.speclib",
        config_file_name="fast.yaml",
        config_params=None,
        software_type="alphadia",
        software="alphadia-1.10.0",
    )

    mock_settings.assert_called_once()
    call_kwargs = mock_settings.call_args.kwargs
    assert call_kwargs["name"] == "plasma_settings"
    assert call_kwargs["version"] == 4
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.Settings")
def test_get_all_settings_excludes_archived(
    mock_settings: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that get_all_settings excludes archived by default."""
    get_all_settings(include_archived=False)

    mock_settings.objects.assert_called_once()
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.Settings")
def test_get_all_settings_includes_archived(
    mock_settings: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that get_all_settings includes archived when requested."""
    get_all_settings(include_archived=True)

    mock_settings.objects.all.assert_called_once()
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.Settings")
@patch("shared.db.interface.Project")
def test_assign_settings_to_project(
    mock_project: MagicMock, mock_settings: MagicMock, mock_connect_db: MagicMock
) -> None:
    """Test that assign_settings_to_project works correctly."""
    mock_project_instance = MagicMock()
    mock_project.objects.get.return_value = mock_project_instance

    mock_settings_instance = MagicMock()
    mock_settings_instance.status = "active"
    mock_settings.objects.get.return_value = mock_settings_instance

    assign_settings_to_project("P1234", "settings_id")

    assert mock_project_instance.settings == mock_settings_instance
    mock_project_instance.save.assert_called_once()
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
        id_="instrument1",
        status="error",
        status_details="some details",
        free_space_gb=123,
    )

    # then
    mock_krakenstatus.assert_called_once_with(
        id="instrument1",
        status="error",
        updated_at_=mock_datetime.now.return_value,
        status_details="some details",
        free_space_gb=123,
        entity_type="instrument",
    )
    mock_connect_db.assert_called_once()


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.RawFile")
def test_get_raw_files_by_instrument_file_status_basic_query(
    mock_raw_file: MagicMock,
    mock_connect_db: MagicMock,  # noqa: ARG001
) -> None:
    """Test that get_raw_files_by_instrument_file_status queries correctly with basic parameters."""
    # given
    mock_query = MagicMock()
    mock_raw_file.objects.filter.return_value = mock_query
    mock_query.order_by.return_value = [MagicMock(), MagicMock()]

    # when
    result = get_raw_files_by_instrument_file_status(InstrumentFileStatus.NEW)

    # then
    mock_raw_file.objects.filter.assert_called_once_with(
        instrument_file_status=InstrumentFileStatus.NEW
    )
    mock_query.order_by.assert_called_once_with("created_at")
    assert len(result) == 2


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.RawFile")
@patch("shared.db.interface.datetime")
def test_get_raw_files_by_instrument_file_status_with_filters(
    mock_datetime: MagicMock,
    mock_raw_file: MagicMock,
    mock_connect_db: MagicMock,  # noqa: ARG001
) -> None:
    """Test that get_raw_files_by_instrument_file_status applies filters correctly."""
    # given
    mock_now = datetime(2023, 1, 1, 12, 0, 0, tzinfo=pytz.UTC)
    mock_datetime.now.return_value = mock_now

    # Create a chain of mock queries
    mock_query1 = MagicMock()
    mock_query2 = MagicMock()
    mock_query3 = MagicMock()
    mock_final_result = [MagicMock()]

    mock_raw_file.objects.filter.return_value = mock_query1
    mock_query1.filter.return_value = mock_query2
    mock_query2.filter.return_value = mock_query3
    mock_query3.order_by.return_value = mock_final_result

    # when
    result = get_raw_files_by_instrument_file_status(
        InstrumentFileStatus.MOVED, instrument_id="test_instrument", min_age_hours=6
    )

    # then
    mock_raw_file.objects.filter.assert_called_once_with(
        instrument_file_status=InstrumentFileStatus.MOVED
    )
    expected_cutoff = mock_now - timedelta(hours=6)
    mock_query1.filter.assert_called_once_with(instrument_id="test_instrument")
    mock_query2.filter.assert_called_once_with(created_at__lt=expected_cutoff)
    mock_query3.order_by.assert_called_once_with("created_at")
    assert result == mock_final_result


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


def test_augment_raw_files_with_metrics_should_return_augmented_files_when_files_have_metrics() -> (
    None
):
    """Test that augment_raw_files_with_metrics returns raw files with their latest metrics."""
    # given
    mock_raw_file1 = MagicMock()
    mock_raw_file1.to_mongo.return_value = {
        "_id": "file1",
        "original_name": "file1.raw",
        "instrument_id": "instrument1",
        "status": "done",
    }

    mock_raw_file2 = MagicMock()
    mock_raw_file2.to_mongo.return_value = {
        "_id": "file2",
        "original_name": "file2.raw",
        "instrument_id": "instrument1",
        "status": "done",
    }

    mock_raw_files = [mock_raw_file1, mock_raw_file2]

    # Mock metrics query chain
    mock_metrics_queryset = MagicMock()
    mock_filtered_queryset = MagicMock()
    mock_ordered_queryset = MagicMock()

    # Create mock metrics
    mock_metric1 = MagicMock()
    mock_metric1.to_mongo.return_value = {
        "_id": "metric1",
        "raw_file": "file1",
        "type": MetricsTypes.ALPHADIA,
        "created_at_": datetime(2023, 1, 1, 12, 0, 0, tzinfo=pytz.UTC),
        "metric_value": 42,
    }

    mock_metric2 = MagicMock()
    mock_metric2.to_mongo.return_value = {
        "_id": "metric2",
        "raw_file": "file2",
        "type": MetricsTypes.MSQC,
        "created_at_": datetime(2023, 1, 2, 12, 0, 0, tzinfo=pytz.UTC),
        "qc_value": 100,
    }

    mock_ordered_queryset.__iter__ = MagicMock(
        return_value=iter([mock_metric1, mock_metric2])
    )
    mock_filtered_queryset.order_by.return_value = mock_ordered_queryset
    mock_metrics_queryset.filter.return_value = mock_filtered_queryset

    with patch("shared.db.interface.Metrics") as mock_metrics_class:
        mock_metrics_class.objects = mock_metrics_queryset

        # when
        result = augment_raw_files_with_metrics(mock_raw_files)

    # then
    assert result == {
        "file1": {
            "_id": "file1",
            "original_name": "file1.raw",
            "instrument_id": "instrument1",
            "status": "done",
            f"metrics_{MetricsTypes.ALPHADIA}": {
                "_id": "metric1",
                "raw_file": "file1",
                "type": MetricsTypes.ALPHADIA,
                "created_at_": datetime(2023, 1, 1, 12, 0, 0, tzinfo=pytz.UTC),
                "metric_value": 42,
            },
        },
        "file2": {
            "_id": "file2",
            "original_name": "file2.raw",
            "instrument_id": "instrument1",
            "status": "done",
            f"metrics_{MetricsTypes.MSQC}": {
                "_id": "metric2",
                "raw_file": "file2",
                "type": MetricsTypes.MSQC,
                "created_at_": datetime(2023, 1, 2, 12, 0, 0, tzinfo=pytz.UTC),
                "qc_value": 100,
            },
        },
    }

    mock_metrics_queryset.filter.assert_called_once_with(
        raw_file__in=["file1", "file2"]
    )
    mock_filtered_queryset.order_by.assert_called_once_with("-created_at_")


def test_augment_raw_files_with_metrics_should_use_only_for_fields_when_fields_specified() -> (
    None
):
    """Test that .only() is called with the correct field list when fields parameter is provided."""
    # given
    mock_raw_file = MagicMock()
    mock_raw_file.to_mongo.return_value = {
        "_id": "file1",
        "original_name": "file1.raw",
        "instrument_id": "instrument1",
        "status": "done",
    }

    mock_raw_files = [mock_raw_file]

    mock_metrics_queryset = MagicMock()
    mock_only_queryset = MagicMock()
    mock_filtered_queryset = MagicMock()
    mock_ordered_queryset = MagicMock()

    mock_metric = MagicMock()
    mock_metric.to_mongo.return_value = {
        "_id": "metric1",
        "raw_file": "file1",
        "type": MetricsTypes.ALPHADIA,
        "created_at_": datetime(2023, 1, 1, 12, 0, 0, tzinfo=pytz.UTC),
        "field1": "value1",
        "field2": "value2",
    }

    mock_ordered_queryset.__iter__ = MagicMock(return_value=iter([mock_metric]))
    mock_filtered_queryset.order_by.return_value = mock_ordered_queryset
    mock_only_queryset.filter.return_value = mock_filtered_queryset
    mock_metrics_queryset.only.return_value = mock_only_queryset

    with patch("shared.db.interface.Metrics") as mock_metrics_class:
        mock_metrics_class.objects = mock_metrics_queryset

        # when
        result = augment_raw_files_with_metrics(
            mock_raw_files, fields=["field1", "field2"]
        )

    # then
    assert result == {
        "file1": {
            "_id": "file1",
            "original_name": "file1.raw",
            "instrument_id": "instrument1",
            "status": "done",
            f"metrics_{MetricsTypes.ALPHADIA}": {
                "_id": "metric1",
                "raw_file": "file1",
                "type": MetricsTypes.ALPHADIA,
                "created_at_": datetime(2023, 1, 1, 12, 0, 0, tzinfo=pytz.UTC),
                "field1": "value1",
                "field2": "value2",
            },
        },
    }

    mock_metrics_queryset.only.assert_called_once_with(
        "raw_file", "type", "field1", "field2"
    )


def test_augment_raw_files_with_metrics_should_return_multiple_metric_types_per_file_when_file_has_multiple_types() -> (
    None
):
    """Test that a raw file with multiple metric types returns both metric types as separate keys."""
    # given
    mock_raw_file = MagicMock()
    mock_raw_file.to_mongo.return_value = {
        "_id": "file1",
        "original_name": "file1.raw",
        "instrument_id": "instrument1",
        "status": "done",
    }

    mock_raw_files = [mock_raw_file]

    mock_metrics_queryset = MagicMock()
    mock_filtered_queryset = MagicMock()
    mock_ordered_queryset = MagicMock()

    mock_metric_alphadia = MagicMock()
    mock_metric_alphadia.to_mongo.return_value = {
        "_id": "metric1",
        "raw_file": "file1",
        "type": MetricsTypes.ALPHADIA,
        "created_at_": datetime(2023, 1, 1, 12, 0, 0, tzinfo=pytz.UTC),
        "alphadia_value": 42,
    }

    mock_metric_msqc = MagicMock()
    mock_metric_msqc.to_mongo.return_value = {
        "_id": "metric2",
        "raw_file": "file1",
        "type": MetricsTypes.MSQC,
        "created_at_": datetime(2023, 1, 1, 13, 0, 0, tzinfo=pytz.UTC),
        "msqc_value": 100,
    }

    mock_ordered_queryset.__iter__ = MagicMock(
        return_value=iter([mock_metric_alphadia, mock_metric_msqc])
    )
    mock_filtered_queryset.order_by.return_value = mock_ordered_queryset
    mock_metrics_queryset.filter.return_value = mock_filtered_queryset

    with patch("shared.db.interface.Metrics") as mock_metrics_class:
        mock_metrics_class.objects = mock_metrics_queryset

        # when
        result = augment_raw_files_with_metrics(mock_raw_files)

    # then
    assert result == {
        "file1": {
            "_id": "file1",
            "original_name": "file1.raw",
            "instrument_id": "instrument1",
            "status": "done",
            f"metrics_{MetricsTypes.ALPHADIA}": {
                "_id": "metric1",
                "raw_file": "file1",
                "type": MetricsTypes.ALPHADIA,
                "created_at_": datetime(2023, 1, 1, 12, 0, 0, tzinfo=pytz.UTC),
                "alphadia_value": 42,
            },
            f"metrics_{MetricsTypes.MSQC}": {
                "_id": "metric2",
                "raw_file": "file1",
                "type": MetricsTypes.MSQC,
                "created_at_": datetime(2023, 1, 1, 13, 0, 0, tzinfo=pytz.UTC),
                "msqc_value": 100,
            },
        },
    }


def test_augment_raw_files_with_metrics_should_return_file_without_metrics_key_when_file_has_no_metrics() -> (
    None
):
    """Test that a raw file with no metrics in the database returns without any metrics_* keys."""
    # given
    mock_raw_file = MagicMock()
    mock_raw_file.to_mongo.return_value = {
        "_id": "file1",
        "original_name": "file1.raw",
        "instrument_id": "instrument1",
        "status": "done",
    }

    mock_raw_files = [mock_raw_file]

    mock_metrics_queryset = MagicMock()
    mock_filtered_queryset = MagicMock()
    mock_ordered_queryset = MagicMock()

    mock_ordered_queryset.__iter__ = MagicMock(return_value=iter([]))
    mock_filtered_queryset.order_by.return_value = mock_ordered_queryset
    mock_metrics_queryset.filter.return_value = mock_filtered_queryset

    with patch("shared.db.interface.Metrics") as mock_metrics_class:
        mock_metrics_class.objects = mock_metrics_queryset

        # when
        result = augment_raw_files_with_metrics(mock_raw_files)

    # then
    assert result == {
        "file1": {
            "_id": "file1",
            "original_name": "file1.raw",
            "instrument_id": "instrument1",
            "status": "done",
        },
    }


def test_augment_raw_files_with_metrics_should_keep_latest_metric_only_when_file_has_multiple_metrics_of_same_type() -> (
    None
):
    """Test that when a file has multiple metrics of the same type, only the latest (by created_at_) is kept."""
    # given
    mock_raw_file = MagicMock()
    mock_raw_file.to_mongo.return_value = {
        "_id": "file1",
        "original_name": "file1.raw",
        "instrument_id": "instrument1",
        "status": "done",
    }

    mock_raw_files = [mock_raw_file]

    mock_metrics_queryset = MagicMock()
    mock_filtered_queryset = MagicMock()
    mock_ordered_queryset = MagicMock()

    mock_metric_latest = MagicMock()
    mock_metric_latest.to_mongo.return_value = {
        "_id": "metric_latest",
        "raw_file": "file1",
        "type": MetricsTypes.ALPHADIA,
        "created_at_": datetime(2023, 1, 2, 12, 0, 0, tzinfo=pytz.UTC),
        "metric_value": 100,
    }

    mock_metric_older = MagicMock()
    mock_metric_older.to_mongo.return_value = {
        "_id": "metric_older",
        "raw_file": "file1",
        "type": MetricsTypes.ALPHADIA,
        "created_at_": datetime(2023, 1, 1, 12, 0, 0, tzinfo=pytz.UTC),
        "metric_value": 42,
    }

    # Ordered by -created_at_, so latest comes first
    mock_ordered_queryset.__iter__ = MagicMock(
        return_value=iter([mock_metric_latest, mock_metric_older])
    )
    mock_filtered_queryset.order_by.return_value = mock_ordered_queryset
    mock_metrics_queryset.filter.return_value = mock_filtered_queryset

    with patch("shared.db.interface.Metrics") as mock_metrics_class:
        mock_metrics_class.objects = mock_metrics_queryset

        # when
        result = augment_raw_files_with_metrics(mock_raw_files)

    # then
    assert result == {
        "file1": {
            "_id": "file1",
            "original_name": "file1.raw",
            "instrument_id": "instrument1",
            "status": "done",
            f"metrics_{MetricsTypes.ALPHADIA}": {
                "_id": "metric_older",
                "raw_file": "file1",
                "type": MetricsTypes.ALPHADIA,
                "created_at_": datetime(2023, 1, 1, 12, 0, 0, tzinfo=pytz.UTC),
                "metric_value": 42,
            },
        },
    }


def test_augment_raw_files_with_metrics_should_return_empty_dict_when_raw_files_is_empty() -> (
    None
):
    """Test that an empty raw files QuerySet returns an empty dictionary."""
    # given
    mock_raw_files = []

    mock_metrics_queryset = MagicMock()

    with patch("shared.db.interface.Metrics") as mock_metrics_class:
        mock_metrics_class.objects = mock_metrics_queryset

        # when
        result = augment_raw_files_with_metrics(mock_raw_files)

    # then
    assert result == {}


def test_augment_raw_files_with_metrics_should_query_correct_fields_when_fields_parameter_provided() -> (
    None
):
    """Test that .only() is called with correct field list when fields parameter is provided, and not called when fields is None."""
    # given
    mock_raw_file = MagicMock()
    mock_raw_file.to_mongo.return_value = {
        "_id": "file1",
        "original_name": "file1.raw",
        "instrument_id": "instrument1",
        "status": "done",
    }

    mock_raw_files = [mock_raw_file]

    mock_metrics_queryset = MagicMock()
    mock_only_queryset = MagicMock()
    mock_filtered_queryset = MagicMock()
    mock_ordered_queryset = MagicMock()

    mock_metric = MagicMock()
    mock_metric.to_mongo.return_value = {
        "_id": "metric1",
        "raw_file": "file1",
        "type": MetricsTypes.ALPHADIA,
        "created_at_": datetime(2023, 1, 1, 12, 0, 0, tzinfo=pytz.UTC),
        "field1": "value1",
    }

    mock_ordered_queryset.__iter__ = MagicMock(return_value=iter([mock_metric]))
    mock_filtered_queryset.order_by.return_value = mock_ordered_queryset
    mock_only_queryset.filter.return_value = mock_filtered_queryset
    mock_metrics_queryset.only.return_value = mock_only_queryset
    mock_metrics_queryset.filter.return_value = mock_filtered_queryset

    with patch("shared.db.interface.Metrics") as mock_metrics_class:
        mock_metrics_class.objects = mock_metrics_queryset

        # when - with fields parameter
        augment_raw_files_with_metrics(mock_raw_files, fields=["field1"])

    # then
    mock_metrics_queryset.only.assert_called_once_with("raw_file", "type", "field1")

    # given - reset mocks
    mock_metrics_queryset.reset_mock()
    mock_ordered_queryset.__iter__ = MagicMock(return_value=iter([mock_metric]))

    with patch("shared.db.interface.Metrics") as mock_metrics_class:
        mock_metrics_class.objects = mock_metrics_queryset

        # when - without fields parameter
        augment_raw_files_with_metrics(mock_raw_files, fields=None)

    # then
    mock_metrics_queryset.only.assert_not_called()
