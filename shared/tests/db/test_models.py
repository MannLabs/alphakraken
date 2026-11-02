"""Tests for the db.models module."""

import pytest

from shared.db.models import parse_file_info_item


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
