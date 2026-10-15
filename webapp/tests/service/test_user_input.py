"""Tests for the user_input module."""

import pytest
from service.user_input import clean_email, clean_initials, parse_user_row


def test_clean_email_and_initials() -> None:
    """Test that email is lowercased and initials are uppercased, both stripped."""
    assert clean_email(" A@B.de ") == "a@b.de"
    assert clean_initials(" ab ") == "AB"


def test_parse_user_row() -> None:
    """Test that a row is parsed and normalized."""
    assert parse_user_row(" A@B.de , ab") == ("a@b.de", "AB")


@pytest.mark.parametrize("bad_row", ["a@b.de", "a@b.de,AB,extra"])
def test_parse_user_row_rejects_wrong_number_of_fields(bad_row: str) -> None:
    """Test that a row without exactly two fields is rejected."""
    with pytest.raises(ValueError, match=f"got '{bad_row}'"):
        parse_user_row(bad_row)
