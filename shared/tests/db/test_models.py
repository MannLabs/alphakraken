"""Tests for the db.models module."""

from datetime import datetime

import pytest
import pytz
from mongoengine import ValidationError

from shared.db.models import AlertIssue, AlertSuppression, User, parse_file_info_item


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
    """Test that an open issue needs alert, key, identifier, details and first_seen."""
    AlertIssue(
        alert="DiskSpaceAlert",
        key="inst1",
        identifier="inst1",
        details="150 GB",
        first_seen=datetime(2024, 1, 1, tzinfo=pytz.UTC),
    ).validate()


def test_alert_issue_rejects_missing_key() -> None:
    """Test that the key is required."""
    with pytest.raises(ValidationError):
        AlertIssue(
            alert="DiskSpaceAlert",
            identifier="inst1",
            details="150 GB",
            first_seen=datetime(2024, 1, 1, tzinfo=pytz.UTC),
        ).validate()


@pytest.mark.parametrize(
    "field", ["error_class", "instrument_id", "context", "message"]
)
def test_alert_suppression_rejects_empty_match_fields(field: str) -> None:
    """Match fields must be a value or the wildcard, never empty."""
    with pytest.raises(ValidationError):
        AlertSuppression(comment="c", **{field: ""}).validate()
