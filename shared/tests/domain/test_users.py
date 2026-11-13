"""Tests for the domain.users module."""

from typing import Any

import pytest

from shared.domain.errors import ValidationError
from shared.domain.inputs import parse
from shared.domain.users import NewUser, UserUpdate


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


def test_user_update_requires_messenger_id() -> None:
    """Leaving the messenger id out is an error, so that it cannot be cleared by accident."""
    with pytest.raises(ValidationError, match="messenger_id: Field required"):
        parse(UserUpdate, initials="JD")


def test_new_user_needs_no_messenger_id() -> None:
    """A new user has no messenger id unless one is given."""
    assert parse(NewUser, email="a@b.de", initials="AB").messenger_id is None


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"email": "no-at-sign"}, "email: String should match pattern"),
        ({"email": "a b@c.de"}, "email: String should match pattern"),
        ({"email": f"{'a' * 125}@b.de"}, "email: String should have at most 128"),
        ({"initials": "J"}, "initials: String should match pattern"),
        ({"initials": "J1"}, "initials: String should match pattern"),
        ({"initials": "toolongxx"}, "initials: String should have at most 8"),
        ({"initials": " "}, "initials: is required."),
        ({"email": ""}, "email: is required."),
        ({"messenger_id": "U" * 65}, "messenger_id: String should have at most 64"),
    ],
)
def test_new_user_rejects_invalid_fields(overrides: dict[str, Any], error: str) -> None:
    """Malformed emails and initials and too long values are rejected."""
    fields = {"email": "a@b.de", "initials": "AB", **overrides}

    with pytest.raises(ValidationError, match=error) as e:
        parse(NewUser, **fields)

    assert len(e.value.errors) == 1
