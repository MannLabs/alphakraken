"""Tests for the user_input module."""

import pytest
from service.user_input import (
    clean_initials,
    clean_messenger_id,
    parse_user_row,
)


def test_clean_initials() -> None:
    """Test that initials are stripped and keep their case."""
    assert clean_initials(" Ab ") == "Ab"


def test_clean_messenger_id() -> None:
    """Test that the messenger id is stripped and empty becomes None."""
    assert clean_messenger_id(" U123 ") == "U123"
    assert clean_messenger_id("  ") is None


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        (" A@B.de , Ab", (" A@B.de ", "Ab", None)),
        (" A@B.de \t Ab", (" A@B.de ", "Ab", None)),
        (" A@B.de , Ab , U123 ", (" A@B.de ", "Ab", "U123")),
        ("a@b.de,Ab,", ("a@b.de", "Ab", None)),
    ],
)
def test_parse_user_row(row: str, expected: tuple[str, str, str | None]) -> None:
    """Test that a comma- or tab-separated row is parsed, the messenger id is optional."""
    assert parse_user_row(row) == expected


@pytest.mark.parametrize(
    "bad_row", ["a@b.de", "a@b.de,AB,U123,extra", "a@b.de,AB\tU123\textra"]
)
def test_parse_user_row_rejects_wrong_number_of_fields(bad_row: str) -> None:
    """Test that a row without two or three fields is rejected."""
    with pytest.raises(ValueError, match="expected 'e-mail,initials"):
        parse_user_row(bad_row)
