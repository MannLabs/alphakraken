"""Tests for the raw files API and its Mongo backend."""

from collections.abc import Iterator
from dataclasses import fields
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import mongoengine
import pymongo.errors
import pytest

from shared.db import models
from shared.db._mongo import raw_files as backend
from shared.db._mongo.connection import ensure_connected
from shared.db._mongo.errors import translate_errors
from shared.db._mongo.mappers import to_raw_file
from shared.db.raw_files import RawFileFilter, count, find
from shared.domain.errors import (
    AlreadyExistsError,
    DatabaseError,
    DatabaseUnavailableError,
    NotFoundError,
    ValidationError,
)
from shared.domain.raw_files import BackupStatus, InstrumentFileStatus, RawFileStatus
from shared.tests.factories import raw_file

NAIVE_TIME = datetime(2024, 1, 2, 3, 4, 5)  # noqa: DTZ001 as stored by pymongo
AWARE_TIME = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)


def _document() -> models.RawFile:
    """A document with every field set, as it comes back from the database."""
    return models.RawFile(
        id="C1_file.raw",
        collision_flag="C1_",
        original_name="file.raw",
        instrument_id="inst1",
        project_id="P1",
        status=RawFileStatus.DONE,
        status_details="details",
        size=1024,
        backup_status=BackupStatus.COPYING_DONE,
        backup_display_base_path="/backup",
        s3_upload_path="bucket/path/",
        instrument_file_status=InstrumentFileStatus.MOVED,
        file_info={"a.raw": [1.0, "hash"], "b.raw": [2.0, "hash2", "etag"]},
        created_at=NAIVE_TIME,
        finalized_at=NAIVE_TIME + timedelta(hours=1),
        created_at_=NAIVE_TIME + timedelta(hours=2),
        updated_at_=NAIVE_TIME + timedelta(hours=3),
    )


# mapper


def test_to_raw_file_maps_all_fields_with_utc_datetimes_and_tuple_file_info() -> None:
    """Every field is carried over, datetimes become UTC-aware, file info items tuples."""
    result = to_raw_file(_document(), file_info_loaded=True)

    assert result == raw_file(
        id="C1_file.raw",
        collision_flag="C1_",
        original_name="file.raw",
        instrument_id="inst1",
        project_id="P1",
        status=RawFileStatus.DONE,
        status_details="details",
        size=1024,
        backup_status=BackupStatus.COPYING_DONE,
        backup_display_base_path="/backup",
        s3_upload_path="bucket/path/",
        instrument_file_status=InstrumentFileStatus.MOVED,
        file_info={"a.raw": (1.0, "hash"), "b.raw": (2.0, "hash2", "etag")},
        created_at=AWARE_TIME,
        finalized_at=AWARE_TIME + timedelta(hours=1),
        created_at_=AWARE_TIME + timedelta(hours=2),
        updated_at_=AWARE_TIME + timedelta(hours=3),
    )
    assert result.has_project


def test_to_raw_file_without_file_info_loaded() -> None:
    """Unloaded file info is None, distinguishable from an empty mapping."""
    result = to_raw_file(_document(), file_info_loaded=False)

    assert result.file_info is None


def test_to_raw_file_keeps_optional_datetimes_none() -> None:
    """A missing optional datetime stays None instead of failing the UTC conversion."""
    document = _document()
    document.finalized_at = None

    assert to_raw_file(document, file_info_loaded=False).finalized_at is None


# filter translation


def test_conditions_of_full_filter() -> None:
    """Each set filter field becomes one query condition."""
    where = RawFileFilter(
        ids=["f1", "f2"],
        instrument_id="inst1",
        instrument_ids=["inst1", "inst2"],
        project_id="P1",
        name_contains="F1",
        created_after=AWARE_TIME,
        created_before=AWARE_TIME + timedelta(days=1),
        updated_after=AWARE_TIME + timedelta(hours=1),
        status=RawFileStatus.ERROR,
        statuses=[RawFileStatus.ERROR, RawFileStatus.QUANTING_FAILED],
        backup_status=BackupStatus.UPLOAD_FAILED,
    )

    assert backend._conditions(where) == {
        "id__in": ["f1", "f2"],
        "instrument_id": "inst1",
        "instrument_id__in": ["inst1", "inst2"],
        "project_id": "P1",
        "id__icontains": "F1",
        "created_at__gte": AWARE_TIME,
        "created_at__lte": AWARE_TIME + timedelta(days=1),
        "updated_at___gte": AWARE_TIME + timedelta(hours=1),
        "status": RawFileStatus.ERROR,
        "status__in": [RawFileStatus.ERROR, RawFileStatus.QUANTING_FAILED],
        "backup_status": BackupStatus.UPLOAD_FAILED,
    }


def test_every_filter_field_has_a_query_key() -> None:
    """A filter field without a query key would raise instead of being ignored."""
    assert set(backend._QUERY_KEYS) == {field.name for field in fields(RawFileFilter)}


def test_conditions_of_empty_filter() -> None:
    """An empty filter matches everything."""
    assert backend._conditions(RawFileFilter()) == {}


def test_conditions_leave_out_unset_fields() -> None:
    """Only the set fields become conditions."""
    where = RawFileFilter(instrument_id="inst1", statuses=[RawFileStatus.ERROR])

    assert backend._conditions(where) == {
        "instrument_id": "inst1",
        "status__in": [RawFileStatus.ERROR],
    }


def test_conditions_raise_for_a_field_without_query_key() -> None:
    """A set field that the backend does not know is an error, not a filter silently dropped."""
    query_keys = {k: v for k, v in backend._QUERY_KEYS.items() if k != "status"}

    with (
        patch.dict(backend._QUERY_KEYS, query_keys, clear=True),
        pytest.raises(KeyError, match="status"),
    ):
        backend._conditions(RawFileFilter(status=RawFileStatus.ERROR))


# find


@pytest.fixture
def mock_objects() -> Iterator[MagicMock]:
    """A mock `RawFile.objects` whose chained calls all return the same queryset mock."""
    query = MagicMock()
    for method in ["order_by", "exclude", "skip", "limit"]:
        getattr(query, method).return_value = query
    query.__iter__.return_value = [_document()]
    with (
        patch("shared.db._mongo.raw_files.ensure_connected"),
        patch("shared.db._mongo.raw_files.models") as mock_models,
    ):
        mock_models.RawFile.objects.return_value = query
        yield mock_models.RawFile.objects


def test_find_with_defaults_excludes_file_info(mock_objects: MagicMock) -> None:
    """By default the query is ordered, without file info, skip and limit."""
    result = find(RawFileFilter(statuses=[RawFileStatus.ERROR]))

    assert result == [to_raw_file(_document(), file_info_loaded=False)]
    mock_objects.assert_called_once_with(status__in=[RawFileStatus.ERROR])
    query = mock_objects.return_value
    query.order_by.assert_called_once_with("-created_at")
    query.exclude.assert_called_once_with("file_info")
    query.skip.assert_not_called()
    query.limit.assert_not_called()


def test_find_with_all_options(mock_objects: MagicMock) -> None:
    """Order, offset, limit and file info are passed on to the query."""
    result = find(
        RawFileFilter(), order="id", limit=1, offset=2, include_file_info=True
    )

    assert result == [to_raw_file(_document(), file_info_loaded=True)]
    query = mock_objects.return_value
    query.order_by.assert_called_once_with("id")
    query.exclude.assert_not_called()
    query.skip.assert_called_once_with(2)
    query.limit.assert_called_once_with(1)


def test_count(mock_objects: MagicMock) -> None:
    """The count applies the same conditions as find."""
    mock_objects.return_value.count.return_value = 3

    assert count(RawFileFilter(project_id="P1")) == 3
    mock_objects.assert_called_once_with(project_id="P1")


def test_find_rejects_unknown_order_field() -> None:
    """Ordering is restricted to the fields of the domain type."""
    with pytest.raises(ValueError, match="Unknown order field: -_id"):
        find(RawFileFilter(), order="-_id")


# error translation


@pytest.mark.parametrize(
    ("raised", "expected"),
    [
        (mongoengine.DoesNotExist("gone"), NotFoundError),
        (mongoengine.NotUniqueError("twice"), AlreadyExistsError),
        (pymongo.errors.DuplicateKeyError("twice"), AlreadyExistsError),
        (pymongo.errors.ServerSelectionTimeoutError("down"), DatabaseUnavailableError),
        (pymongo.errors.OperationFailure("not authorized"), DatabaseError),
        (mongoengine.ConnectionFailure("alias registered"), DatabaseError),
    ],
)
def test_translate_errors(raised: Exception, expected: type[Exception]) -> None:
    """Provider exceptions become domain errors with the original message."""

    @translate_errors
    def fail() -> None:
        raise raised

    with pytest.raises(expected, match=str(raised)):
        fail()


def test_translate_errors_lists_one_message_per_invalid_field() -> None:
    """A mongoengine validation error carries one message per field."""
    document = models.User(email="no-at-sign", initials="1")

    @translate_errors
    def fail() -> None:
        document.validate()

    with pytest.raises(ValidationError) as excinfo:
        fail()
    assert sorted(message.split(":")[0] for message in excinfo.value.errors) == [
        "email",
        "initials",
    ]


def test_translate_errors_passes_results_and_other_exceptions_through() -> None:
    """Unrelated exceptions are not translated."""

    @translate_errors
    def compute(x: int) -> int:
        if x < 0:
            raise ValueError("negative")
        return x * 2

    assert compute(2) == 4
    with pytest.raises(ValueError, match="negative"):
        compute(-1)


# connection


@pytest.fixture(autouse=True)
def _forget_connection() -> None:
    ensure_connected.cache_clear()


@patch("shared.db._mongo.connection.connect_db")
def test_ensure_connected_registers_once(mock_connect_db: MagicMock) -> None:
    """A successful registration is not repeated."""
    ensure_connected()
    ensure_connected()

    mock_connect_db.assert_called_once_with(raise_on_error=True)


@patch("shared.db._mongo.connection.connect_db")
def test_ensure_connected_retries_after_failure(mock_connect_db: MagicMock) -> None:
    """A failed registration is retried on the next call."""
    mock_connect_db.side_effect = [mongoengine.ConnectionFailure("boom"), None]

    with pytest.raises(mongoengine.ConnectionFailure):
        ensure_connected()
    ensure_connected()

    assert mock_connect_db.call_count == 2
