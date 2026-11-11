"""Tests for the user_input module."""

import pytest
from service.user_input import parse_user_row


@pytest.mark.parametrize(
    ("row", "expected"),
    [
        (" A@B.de , Ab", (" A@B.de ", " Ab", None)),
        (" A@B.de \t Ab", (" A@B.de ", " Ab", None)),
        (" A@B.de , Ab , U123 ", (" A@B.de ", " Ab ", " U123 ")),
        ("a@b.de,Ab,", ("a@b.de", "Ab", "")),
    ],
)
def test_parse_user_row(row: str, expected: tuple[str, str, str | None]) -> None:
    """Test that a comma- or tab-separated row is split as it is, the messenger id is optional."""
    assert parse_user_row(row) == expected


@pytest.mark.parametrize(
    "bad_row", ["a@b.de", "a@b.de,AB,U123,extra", "a@b.de,AB\tU123\textra"]
)
def test_parse_user_row_rejects_wrong_number_of_fields(bad_row: str) -> None:
    """Test that a row without two or three fields is rejected."""
    with pytest.raises(ValueError, match="expected 'e-mail,initials"):
        parse_user_row(bad_row)
