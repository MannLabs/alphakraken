"""Tests for the metrics API and its Mongo backend."""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from shared.db._mongo import metrics as backend
from shared.db._mongo import models
from shared.db.metrics import add, find, latest_by_raw_file
from shared.domain.errors import InvalidInputError
from shared.keys import MetricsTypes
from shared.tests.factories import metrics

NAIVE_TIME = datetime(2024, 1, 2, 3, 4, 5)  # noqa: DTZ001 as stored by pymongo
AWARE_TIME = datetime(2024, 1, 2, 3, 4, 5, tzinfo=UTC)


def _document(
    raw_file_id: str = "file.raw",
    metrics_type: str = MetricsTypes.ALPHADIA,
    created_at_: datetime = NAIVE_TIME,
    values: dict[str, Any] | None = None,
) -> models.Metrics:
    return models.Metrics(
        raw_file=models.RawFile(id=raw_file_id, original_name=raw_file_id),
        type=metrics_type,
        settings_name="fallback",
        settings_version=1,
        relative_output_path="P1/out_file.raw/fallback_v1",
        output_display_base_path="/output",
        created_at_=created_at_,
        **(values or {}),
    )


# mapping


def test_to_metrics_maps_metadata_and_values() -> None:
    """Metadata becomes fields, everything else a value, datetimes UTC-aware."""
    result = backend._to_metrics(
        _document(values={"proteins": 5000, "precursors": [1, 2]})
    )

    assert result == metrics(
        created_at=AWARE_TIME, values={"proteins": 5000, "precursors": [1, 2]}
    )


def test_to_metrics_without_output_path() -> None:
    """Metrics written before the output path existed have None there."""
    document = _document(values={"proteins": 1})
    document.relative_output_path = None
    document.output_display_base_path = None

    result = backend._to_metrics(document)

    assert result.relative_output_path is None
    assert result.output_display_base_path is None


# latest_by_raw_file


@pytest.fixture
def mock_objects() -> Iterator[MagicMock]:
    """A mock `Metrics.objects` whose chained calls all return the same queryset mock."""
    query = MagicMock()
    for method in ["order_by", "only"]:
        getattr(query, method).return_value = query
    # newest first, as the backend orders them
    query.__iter__.return_value = [
        _document("f1", MetricsTypes.ALPHADIA, NAIVE_TIME, values={"proteins": 2}),
        _document("f1", MetricsTypes.MSQC, NAIVE_TIME, values={"pressure": 100.0}),
        _document(
            "f1",
            MetricsTypes.ALPHADIA,
            NAIVE_TIME - timedelta(days=1),
            values={"proteins": 1},
        ),
        _document("f2", MetricsTypes.ALPHADIA, NAIVE_TIME, values={"proteins": 3}),
    ]
    with (
        patch("shared.db._mongo.connection.ensure_connected"),
        patch("shared.db._mongo.metrics.models") as mock_models,
    ):
        mock_models.Metrics.objects.return_value = query
        yield mock_models.Metrics.objects


def test_latest_by_raw_file_keeps_newest_per_type(mock_objects: MagicMock) -> None:
    """Per raw file and type, the newest metrics win; all values are loaded by default."""
    result = latest_by_raw_file(["f1", "f2", "f3"])

    assert {k: {t: m.values for t, m in v.items()} for k, v in result.items()} == {
        "f1": {
            MetricsTypes.ALPHADIA: {"proteins": 2},
            MetricsTypes.MSQC: {"pressure": 100.0},
        },
        "f2": {MetricsTypes.ALPHADIA: {"proteins": 3}},
    }
    mock_objects.assert_called_once_with(raw_file__in=["f1", "f2", "f3"])
    query = mock_objects.return_value
    query.order_by.assert_called_once_with("-created_at_")
    query.only.assert_not_called()


def test_latest_by_raw_file_projects_fields_with_metadata(
    mock_objects: MagicMock,
) -> None:
    """A projection always loads the metadata, plus the fields."""
    latest_by_raw_file(["f1"], fields=["gradient_length"])

    mock_objects.return_value.only.assert_called_once_with(
        *backend._METADATA_FIELDS, "gradient_length"
    )


def test_find_returns_all_metrics_newest_first(mock_objects: MagicMock) -> None:
    """Unlike the latest per type, every metrics entry is returned."""
    result = find(["f1", "f2"], fields=[])

    assert [(m.raw_file_id, m.type, m.values) for m in result] == [
        ("f1", MetricsTypes.ALPHADIA, {"proteins": 2}),
        ("f1", MetricsTypes.MSQC, {"pressure": 100.0}),
        ("f1", MetricsTypes.ALPHADIA, {"proteins": 1}),
        ("f2", MetricsTypes.ALPHADIA, {"proteins": 3}),
    ]
    mock_objects.assert_called_once_with(raw_file__in=["f1", "f2"])
    # no values requested: only the metadata is loaded
    mock_objects.return_value.only.assert_called_once_with(*backend._METADATA_FIELDS)


def test_find_by_creation_time(mock_objects: MagicMock) -> None:
    """Metrics can be selected by when they were written, without sending raw file ids."""
    latest_by_raw_file(created_after=AWARE_TIME)

    mock_objects.assert_called_once_with(created_at___gte=AWARE_TIME)


def test_find_requires_a_condition() -> None:
    """Loading all metrics of the database is not offered."""
    with pytest.raises(ValueError, match="raw_file_ids or created_after"):
        find()


@patch("shared.db._mongo.connection.ensure_connected")
@patch("shared.db._mongo.metrics.models")
def test_add(mock_models: MagicMock, mock_connected: MagicMock) -> None:  # noqa: ARG001
    """Metrics are stored for an existing raw file, with the values as fields of the document."""
    add(
        "f1",
        metrics_type="alphadia",
        values={"proteins": 5000},
        settings_name="fallback",
        settings_version=1,
        relative_output_path="P1/out_f1/fallback_v1",
        output_display_base_path="/output",
    )

    mock_models.RawFile.objects.get.assert_called_once_with(id="f1")
    mock_models.Metrics.assert_called_once_with(
        raw_file=mock_models.RawFile.objects.get.return_value,
        type="alphadia",
        settings_name="fallback",
        settings_version=1,
        relative_output_path="P1/out_f1/fallback_v1",
        output_display_base_path="/output",
        proteins=5000,
    )
    # never overwrites existing metrics
    mock_models.Metrics.return_value.save.assert_called_once_with(force_insert=True)


@pytest.mark.parametrize(
    ("name", "error"),
    [
        ("type", "values: 'type' is a field or method of a metrics entry."),
        (
            "created_at_",
            "values: 'created_at_' is a field or method of a metrics entry.",
        ),
        ("id", "values: 'id' is a field or method of a metrics entry."),
        ("validate", "values: 'validate' is a field or method of a metrics entry."),
        ("objects", "values: 'objects' is a field or method of a metrics entry."),
        ("_id", "values: '_id' must not start with '_'."),
        ("_private", "values: '_private' must not start with '_'."),
        ("raw.proteins", "values: 'raw.proteins' must not contain '.' or '$'."),
        ("$set", "values: '$set' must not contain '.' or '$'."),
    ],
)
@patch("shared.db._mongo.connection.ensure_connected")
@patch.object(models.Metrics, "save")
def test_add_rejects_value_names_that_cannot_be_stored(
    mock_save: MagicMock,
    mock_connected: MagicMock,  # noqa: ARG001
    name: str,
    error: str,
) -> None:
    """A value must not replace a field or method of the document, nor be dropped or read as a path or an operator."""
    # the real model, as the check reads its attributes; rejected before the database is asked
    with pytest.raises(InvalidInputError) as e:
        add(
            "f1",
            metrics_type="alphadia",
            values={name: 1, "proteins": 5000},
            settings_name="fallback",
            settings_version=1,
            relative_output_path="P1/out_f1/fallback_v1",
            output_display_base_path="/output",
        )

    assert e.value.errors == [error]
    mock_save.assert_not_called()
