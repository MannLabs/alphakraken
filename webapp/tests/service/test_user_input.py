"""Tests for the user_input module."""

import pytest
from service.user_input import clean_initials, parse_user_row


def test_clean_initials() -> None:
    """Test that initials are stripped and keep their case."""
    assert clean_initials(" Ab ") == "Ab"


@pytest.mark.parametrize("row", [" A@B.de , Ab", " A@B.de \t Ab"])
def test_parse_user_row(row: str) -> None:
    """Test that a comma- or tab-separated row is parsed and the initials are normalized."""
    assert parse_user_row(row) == (" A@B.de ", "Ab")


@pytest.mark.parametrize("bad_row", ["a@b.de", "a@b.de,AB,extra", "a@b.de,AB\textra"])
def test_parse_user_row_rejects_wrong_number_of_fields(bad_row: str) -> None:
    """Test that a row without exactly two fields is rejected."""
    with pytest.raises(ValueError, match="expected 'e-mail,initials'"):
        parse_user_row(bad_row)
