"""Tests for the raw files API and its Mongo backend."""

from collections.abc import Iterator
from dataclasses import fields
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, patch

import mongoengine
import pymongo.errors
import pytest
from mongoengine import Q

from shared import db
from shared.db import models
from shared.db._mongo import raw_files as backend
from shared.db._mongo.connection import ensure_connected
from shared.db._mongo.errors import translate_errors
from shared.db._mongo.mappers import to_raw_file
from shared.db.raw_files import (
    _ORDER_FIELDS,
    RawFileFilter,
    add,
    count,
    count_by_status,
    delete,
    find,
    get,
    latest_per_instrument,
    update,
)
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


def test_conditions_of_full_filter_resolve_against_the_model() -> None:
    """Every query key names a field and an operator the model knows."""
    where = RawFileFilter(
        ids=["f1"],
        original_names=["F1.raw"],
        instrument_id="inst1",
        instrument_ids=["inst1"],
        project_id="P1",
        name_contains="F1",
        created_after=AWARE_TIME,
        created_before=AWARE_TIME,
        updated_after=AWARE_TIME,
        status=RawFileStatus.ERROR,
        statuses=[RawFileStatus.ERROR],
        statuses_not=[RawFileStatus.DONE],
        backup_status=BackupStatus.UPLOAD_FAILED,
        instrument_file_status=InstrumentFileStatus.MOVED,
        instrument_file_statuses_not=[InstrumentFileStatus.PURGED],
    )
    conditions = backend._conditions(where)
    assert set(conditions) == set(backend._QUERY_KEYS.values())

    # raises InvalidQueryError on an unknown field or operator
    Q(**conditions).to_query(models.RawFile)


def test_every_order_field_is_a_model_field() -> None:
    """Ordering by any field of the domain type is valid for the model."""
    # raises InvalidQueryError on an unknown field
    Q(**dict.fromkeys(_ORDER_FIELDS)).to_query(models.RawFile)


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


def test_latest_per_instrument(mock_objects: MagicMock) -> None:
    """Per instrument with recent files, the newest ones are loaded without file info."""
    query = mock_objects.return_value
    query.distinct.return_value = ["inst1", "inst2"]
    query.filter.return_value = query

    result = latest_per_instrument(3, created_after=AWARE_TIME)

    expected = [to_raw_file(_document(), file_info_loaded=False)]
    assert result == {"inst1": expected, "inst2": expected}
    mock_objects.assert_called_once_with(created_at__gte=AWARE_TIME)
    query.distinct.assert_called_once_with("instrument_id")
    assert [c.kwargs for c in query.filter.call_args_list] == [
        {"instrument_id": "inst1"},
        {"instrument_id": "inst2"},
    ]
    query.order_by.assert_called_with("-created_at")
    query.exclude.assert_called_with("file_info")
    query.limit.assert_called_with(3)


def test_count_by_status(mock_objects: MagicMock) -> None:
    """The files of an instrument are counted per status in the database."""
    mock_objects.return_value.aggregate.return_value = [
        {"_id": "quanting", "count": 3},
        {"_id": "copying", "count": 1},
    ]

    result = count_by_status(
        RawFileFilter(instrument_id="inst1", statuses_not=["done"])
    )

    assert result == {"quanting": 3, "copying": 1}
    mock_objects.assert_called_once_with(instrument_id="inst1", status__nin=["done"])
    mock_objects.return_value.aggregate.assert_called_once_with(
        [{"$group": {"_id": "$status", "count": {"$sum": 1}}}]
    )


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


@patch("shared.db._mongo.connection.get_db")
@patch("shared.db._mongo.connection.connect_db")
def test_ping(mock_connect_db: MagicMock, mock_get_db: MagicMock) -> None:
    """A ping connects and asks the server."""
    db.ping()

    mock_connect_db.assert_called_once()
    mock_get_db.return_value.command.assert_called_once_with("ping")


@patch("shared.db._mongo.connection.get_db")
@patch("shared.db._mongo.connection.connect_db")
def test_ping_raises_when_database_is_unreachable(
    mock_connect_db: MagicMock,  # noqa: ARG001
    mock_get_db: MagicMock,
) -> None:
    """An unreachable server is reported as the domain error clients catch."""
    mock_get_db.return_value.command.side_effect = (
        pymongo.errors.ServerSelectionTimeoutError("down")
    )

    with pytest.raises(DatabaseUnavailableError, match="down"):
        db.ping()


# single raw file reads and writes


def test_get(mock_objects: MagicMock) -> None:
    """A raw file is loaded by id, without file info by default."""
    query = mock_objects.return_value
    query.first.return_value = _document()

    assert get("C1_file.raw") == to_raw_file(_document(), file_info_loaded=False)
    mock_objects.assert_called_once_with(id="C1_file.raw")
    query.exclude.assert_called_once_with("file_info")


def test_get_with_file_info(mock_objects: MagicMock) -> None:
    """The file info is loaded on request."""
    query = mock_objects.return_value
    query.first.return_value = _document()

    result = get("C1_file.raw", include_file_info=True)

    assert result.get_file_info() == {
        "a.raw": (1.0, "hash"),
        "b.raw": (2.0, "hash2", "etag"),
    }
    query.exclude.assert_not_called()


def test_get_raises_if_missing(mock_objects: MagicMock) -> None:
    """An unknown raw file is an error, where the legacy lookup returned None."""
    mock_objects.return_value.first.return_value = None

    with pytest.raises(NotFoundError, match="unknown.raw"):
        get("unknown.raw")


def test_get_file_info_raises_if_not_loaded() -> None:
    """Reading file info that was not loaded is an error, not an empty result."""
    with pytest.raises(ValueError, match="was not loaded"):
        raw_file(file_info=None).get_file_info()


@pytest.mark.parametrize(
    ("collision_flag", "expected_id"),
    [(None, "file.raw"), ("123-", "123-file.raw")],
)
@patch("shared.db._mongo.raw_files.add")
def test_add_prefixes_the_id_with_the_collision_flag(
    mock_backend_add: MagicMock, collision_flag: str | None, expected_id: str
) -> None:
    """The id is the file name, prefixed with the collision flag if there is one."""
    result = add(
        "file.raw",
        collision_flag,
        project_id="P1",
        status=RawFileStatus.QUEUED_FOR_MONITORING,
        instrument_file_status=InstrumentFileStatus.INITIAL,
        instrument_id="inst1",
        creation_ts=43.0,
    )

    assert result == expected_id
    mock_backend_add.assert_called_once_with(
        expected_id,
        collision_flag=collision_flag,
        original_name="file.raw",
        project_id="P1",
        instrument_id="inst1",
        status=RawFileStatus.QUEUED_FOR_MONITORING,
        instrument_file_status=InstrumentFileStatus.INITIAL,
        created_at=datetime(1970, 1, 1, 0, 0, 43, tzinfo=UTC),
    )


def test_backend_add_never_overwrites() -> None:
    """The backend inserts the raw file, failing if the id exists."""
    with (
        patch("shared.db._mongo.raw_files.ensure_connected"),
        patch("shared.db._mongo.raw_files.models") as mock_models,
    ):
        backend.add("file.raw", original_name="file.raw")

    mock_models.RawFile.assert_called_once_with(id="file.raw", original_name="file.raw")
    mock_models.RawFile.return_value.save.assert_called_once_with(force_insert=True)


@patch("shared.db._mongo.raw_files.update")
def test_update_passes_only_the_given_fields(mock_backend_update: MagicMock) -> None:
    """Fields left out keep their value; None is a value, e.g. to clear the status details."""
    update("file.raw", new_status=RawFileStatus.DONE, status_details=None, size=7)

    mock_backend_update.assert_called_once_with(
        "file.raw",
        {"status": RawFileStatus.DONE, "status_details": None, "size": 7},
    )


@patch("shared.db._mongo.raw_files.datetime")
def test_backend_update_sets_the_update_time(
    mock_datetime: MagicMock, mock_objects: MagicMock
) -> None:
    """The backend stamps every update."""
    backend.update("file.raw", {"status": RawFileStatus.DONE})

    mock_objects.with_id.assert_called_once_with("file.raw")
    mock_objects.with_id.return_value.update.assert_called_once_with(
        updated_at_=mock_datetime.now.return_value, status=RawFileStatus.DONE
    )


def test_backend_update_raises_if_missing(mock_objects: MagicMock) -> None:
    """Updating an unknown raw file is an error."""
    mock_objects.with_id.return_value = None

    with pytest.raises(NotFoundError, match="unknown.raw"):
        backend.update("unknown.raw", {"status": RawFileStatus.DONE})


def test_delete(mock_objects: MagicMock) -> None:
    """The raw file with the given id is removed."""
    delete("file.raw")

    mock_objects.assert_called_once_with(id="file.raw")
    mock_objects.return_value.delete.assert_called_once()


def test_original_names_match_exactly_ignoring_case() -> None:
    """Names are matched as a whole, with special characters taken literally, in any case."""
    (pattern,) = backend._conditions(RawFileFilter(original_names=["File(1).raw"]))[
        "original_name__in"
    ]

    assert pattern.match("file(1).RAW")
    assert not pattern.match("File(1).raw.bak")
    assert not pattern.match("File1.raw")
