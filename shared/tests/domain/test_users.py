"""Tests for the domain.users module."""

from typing import Any

import pytest

from shared.domain.errors import InvalidInputError
from shared.domain.inputs import parse
from shared.domain.users import (
    USER_EMAIL_REGEX,
    USER_INITIALS_REGEX,
    NewUser,
    UserUpdate,
    normalize_email,
)


def test_new_user_cleans_fields() -> None:
    """Strings are stripped, the email is lower-cased, the initials keep their case."""
    new = parse(NewUser, email=" Jane.Doe@B.de ", initials=" Jd ", messenger_id=" U1 ")

    assert new.model_dump() == {
        "email": "jane.doe@b.de",
        "initials": "Jd",
        "messenger_id": "U1",
    }


@pytest.mark.parametrize("messenger_id", ["", "  ", None])
def test_user_update_drops_empty_messenger_id(messenger_id: str | None) -> None:
    """A blank messenger id becomes None."""
    assert parse(UserUpdate, initials="JD", messenger_id=messenger_id) == UserUpdate(
        initials="JD", messenger_id=None
    )


def test_new_user_needs_no_messenger_id() -> None:
    """A new user has no messenger id unless one is given."""
    assert parse(NewUser, email="a@b.de", initials="AB").messenger_id is None


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"email": "no-at-sign"}, "email: must be an email address"),
        ({"email": "a b@c.de"}, "email: must be an email address"),
        # a zero-width space and a Cyrillic a (U+0430) look like 'jane@b.de'
        ({"email": "ja\u200bne@b.de"}, "email: must be an email address"),
        ({"email": "j\u0430ne@b.de"}, "email: must be an email address"),
        ({"email": "a@b@c.de"}, "email: must be an email address"),
        ({"email": f"{'a' * 125}@b.de"}, "email: String should have at most 128"),
        ({"initials": "J"}, "initials: must be 2 to 8 letters."),
        ({"initials": "J1"}, "initials: must be 2 to 8 letters."),
        ({"initials": "toolongxx"}, "initials: String should have at most 8"),
        ({"initials": " "}, "initials: is required."),
        ({"email": ""}, "email: is required."),
        ({"messenger_id": "U" * 513}, "messenger_id: String should have at most 512"),
    ],
)
def test_new_user_rejects_invalid_fields(overrides: dict[str, Any], error: str) -> None:
    """Malformed emails and initials and too long values are rejected."""
    fields = {"email": "a@b.de", "initials": "AB", **overrides}

    with pytest.raises(InvalidInputError, match=error) as e:
        parse(NewUser, **fields)

    assert len(e.value.errors) == 1


@pytest.mark.parametrize(
    ("field", "pattern"),
    [("email", USER_EMAIL_REGEX), ("initials", USER_INITIALS_REGEX)],
)
def test_new_user_schema_shows_pattern(field: str, pattern: str) -> None:
    """The JSON schema shows the pattern that the field is checked against."""
    assert NewUser.model_json_schema()["properties"][field]["pattern"] == pattern


def test_normalize_email_strips_and_lower_cases() -> None:
    """An email is looked up as it is stored."""
    assert normalize_email(" Jane@B.de ") == "jane@b.de"


@pytest.mark.parametrize("email", [None, 1, ["a@b.de"]])
def test_normalize_email_rejects_non_string_with_the_domain_error(email: Any) -> None:
    """Clients only catch the errors of `shared.domain.errors`."""
    with pytest.raises(InvalidInputError) as e:
        normalize_email(email)

    assert e.value.errors == ["email: must be a string."]
