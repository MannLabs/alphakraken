"""Tests for the db.models module."""

from datetime import UTC, datetime

import pytest
import pytz
from mongoengine import Document, ValidationError

from shared.db.models import (
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
    parse_file_info_item,
)
from shared.domain.settings import split_unique_settings_name


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
    ],
)
def test_split_unique_settings_name(
    unique_name: str, expected: tuple[str, int]
) -> None:
    """Name and version are recovered from the unique name."""
    assert split_unique_settings_name(unique_name) == expected


@pytest.mark.parametrize(
    "unique_name",
    ["fallback", "fallback_v", "fallback_v0", "fallback_vx", "fall-back_v1", "_v1"],
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
