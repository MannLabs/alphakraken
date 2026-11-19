"""Tests for the db.models module."""

from datetime import UTC, datetime
from typing import get_args

import pytest
import pytz
from mongoengine import Document, ValidationError

from shared.db._mongo.models import (
    AlertIssue,
    AlertSuppression,
    KrakenStatus,
    Metrics,
    Project,
    ProjectSettings,
    RawFile,
    Settings,
    SlurmJob,
    User,
)
from shared.domain.alerts import NewSuppression
from shared.domain.inputs import InputModel, max_length
from shared.domain.projects import NewAssignment, NewProject
from shared.domain.raw_files import RawFileUpdate, parse_file_info_item
from shared.domain.settings import NewSettings, split_unique_settings_name
from shared.domain.users import NewUser


def test_parse_file_info_item_empty_tuple() -> None:
    """Test parse_file_info_item with empty tuple."""
    result = parse_file_info_item(())  # type: ignore[invalid-argument-type]

    assert result == ()


def test_parse_file_info_item_two_tuple_without_etag() -> None:
    """Test parse_file_info_item with 2-tuple (size, hash) when not requesting ETag."""
    item = (1024.0, "abc123hash")

    result = parse_file_info_item(item)

    assert result == (1024.0, "abc123hash")


def test_parse_file_info_item_three_tuple_with_etag() -> None:
    """Test parse_file_info_item with 3-tuple returns full tuple when ETag requested."""
    item = (1024.0, "abc123hash", "etag123--500")

    result = parse_file_info_item(item)

    assert result == (1024.0, "abc123hash")


def test_parse_file_info_item_invalid_length_raises() -> None:
    """Test parse_file_info_item raises with invalid tuple length."""
    item = (1024.0,)

    with pytest.raises(ValueError, match="Invalid file_info item length 1"):
        parse_file_info_item(item)  # type: ignore[invalid-argument-type]


def test_unique_name() -> None:
    """The unique name joins name and version."""
    assert Settings(name="fallback", version=7).unique_name == "fallback_v7"


@pytest.mark.parametrize(
    ("unique_name", "expected"),
    [
        ("fallback_v7", ("fallback", 7)),
        ("fallback_v2_v3", ("fallback_v2", 3)),
        ("a_v10", ("a", 10)),
        ("dia-default-v1.8_v1", ("dia-default-v1.8", 1)),
    ],
)
def test_split_unique_settings_name(
    unique_name: str, expected: tuple[str, int]
) -> None:
    """Name and version are recovered from the unique name."""
    assert split_unique_settings_name(unique_name) == expected


@pytest.mark.parametrize(
    "unique_name",
    ["fallback", "fallback_v", "fallback_v0", "fallback_vx", "_v1", "fallback_v7\n"],
)
def test_split_unique_settings_name_raises_on_invalid(unique_name: str) -> None:
    """Strings not of the form <name>_v<version> are rejected."""
    with pytest.raises(ValueError):
        split_unique_settings_name(unique_name)


@pytest.mark.parametrize("initials", ["AB", "ab", "MaS", "ABCDEFGH"])
def test_user_accepts_initials_of_any_case(initials: str) -> None:
    """Test that initials keep their case and pass validation."""
    User(email="a@b.de", initials=initials).validate()


@pytest.mark.parametrize("initials", ["A-B", "A B", "A1", "A", "ABCDEFGHI"])
def test_user_rejects_invalid_initials(initials: str) -> None:
    """Test that initials other than 2 to 8 letters are rejected."""
    with pytest.raises(ValidationError):
        User(email="a@b.de", initials=initials).validate()


def test_alert_issue_validates_with_required_fields() -> None:
    """Test that an issue needs alert, key and first_seen."""
    AlertIssue(
        alert="DiskSpaceAlert",
        key="inst1",
        first_seen=datetime(2024, 1, 1, tzinfo=pytz.UTC),
    ).validate()


def test_alert_issue_rejects_missing_key() -> None:
    """Test that the key is required."""
    with pytest.raises(ValidationError):
        AlertIssue(
            alert="DiskSpaceAlert",
            first_seen=datetime(2024, 1, 1, tzinfo=pytz.UTC),
        ).validate()


@pytest.mark.parametrize("field", ["alert", "instrument_id", "identifier", "details"])
def test_alert_suppression_rejects_empty_match_fields(field: str) -> None:
    """Match fields must be a value or the wildcard, never empty."""
    with pytest.raises(ValidationError):
        AlertSuppression(comment="c", **{field: ""}).validate()


@pytest.mark.parametrize(
    ("document", "field"),
    [
        (RawFile(id="file.raw", original_name="file.raw"), "created_at_"),
        (RawFile(id="file.raw", original_name="file.raw"), "updated_at_"),
        (Metrics(), "created_at_"),
        (User(email="a@b.c", initials="ab"), "created_at_"),
        (AlertSuppression(comment="c"), "created_at_"),
        (Project(id="P001", name="p"), "created_at_"),
        (Settings(name="s"), "created_at_"),
        (ProjectSettings(), "created_at_"),
        (KrakenStatus(id="inst1"), "updated_at_"),
        (SlurmJob(id="c/1", cluster_id="c"), "updated_at_"),
    ],
)
def test_audit_timestamp_defaults_are_utc_aware(document: Document, field: str) -> None:
    """Every stored timestamp is explicitly UTC, so that no reader has to guess."""
    assert getattr(document, field).tzinfo is UTC


# each input type with the model it is stored in, and the model's name of a field if it differs
_STORED_AS: list[tuple[type[InputModel], type[Document], dict[str, str]]] = [
    (NewUser, User, {}),
    (NewSuppression, AlertSuppression, {}),
    (NewProject, Project, {"project_id": "id"}),
    (NewSettings, Settings, {}),
    (RawFileUpdate, RawFile, {}),
]


def _has_max_length(metadata: list) -> bool:
    return any(getattr(m, "max_length", None) is not None for m in metadata)


def _length_limited_fields() -> list:
    """Return input type, model and both names of every length-limited field of an input type."""
    return [
        pytest.param(
            input_type,
            field,
            model,
            renamed.get(field, field),
            id=f"{input_type.__name__}.{field}",
        )
        for input_type, model, renamed in _STORED_AS
        for field, info in input_type.model_fields.items()
        if _has_max_length(info.metadata)
    ]


@pytest.mark.parametrize(
    ("input_type", "field", "model", "model_field"), _length_limited_fields()
)
def test_input_type_and_model_have_the_same_max_length(
    input_type: type[InputModel], field: str, model: type[Document], model_field: str
) -> None:
    """A value passing the input type must not fail in the model, whose error names the model's fields."""
    assert max_length(input_type, field) == getattr(model, model_field).max_length


def _bound(metadata: list, name: str) -> object:
    return next((getattr(m, name) for m in metadata if hasattr(m, name)), None)


def _bounded_fields() -> list:
    """Return input type, model and both names of every numerically bounded field of an input type."""
    return [
        pytest.param(
            input_type,
            field,
            model,
            renamed.get(field, field),
            id=f"{input_type.__name__}.{field}",
        )
        for input_type, model, renamed in _STORED_AS
        for field, info in input_type.model_fields.items()
        if _bound(info.metadata, "ge") is not None
        or _bound(info.metadata, "le") is not None
    ]


@pytest.mark.parametrize(
    ("input_type", "field", "model", "model_field"), _bounded_fields()
)
def test_input_type_and_model_have_the_same_numeric_bounds(
    input_type: type[InputModel], field: str, model: type[Document], model_field: str
) -> None:
    """A number passing the input type must not fail in the model."""
    metadata = input_type.model_fields[field].metadata
    stored = getattr(model, model_field)

    assert (_bound(metadata, "ge"), _bound(metadata, "le")) == (
        stored.min_value,
        stored.max_value,
    )


@pytest.mark.parametrize(
    "field",
    ["scopes", "excluded_scopes", "raw_file_id_filter", "raw_file_id_exclude_filter"],
)
def test_assignment_entries_have_the_same_max_length_in_input_type_and_model(
    field: str,
) -> None:
    """The entries of the scope and filter lists are limited alike."""
    (entry_type,) = get_args(NewAssignment.model_fields[field].annotation)
    (entry_limit,) = (
        m.max_length
        for info in entry_type.__metadata__
        for m in info.metadata
        if getattr(m, "max_length", None) is not None
    )

    assert entry_limit == getattr(ProjectSettings, field).field.max_length
