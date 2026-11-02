"""Tests for the db.models module."""

import pytest

from shared.db.models import (
    Settings,
    parse_file_info_item,
    split_unique_settings_name,
)


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
