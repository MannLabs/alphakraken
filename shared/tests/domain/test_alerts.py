"""Tests for the domain.alerts module."""

from typing import Any

import pytest

from shared.domain.alerts import NewSuppression
from shared.domain.errors import ValidationError
from shared.domain.inputs import parse

VALID: dict[str, Any] = {
    "alert": "PumpPressureAlert",
    "instrument_id": "*",
    "identifier": "*",
    "details": "*",
    "comment": "pump under maintenance",
}


def test_new_suppression_accepts_restricted_suppression() -> None:
    """A suppression restricting at least one field with a comment is valid, without expiry and creator."""
    new = parse(NewSuppression, **VALID)

    assert new.model_dump() == {**VALID, "expires_at": None, "created_by": None}


def test_new_suppression_strips_and_drops_empty_creator() -> None:
    """Strings are stripped, a blank creator becomes None."""
    new = parse(
        NewSuppression,
        **{
            **VALID,
            "alert": " PumpPressureAlert ",
            "comment": " why ",
            "created_by": " ",
        },
    )

    assert (new.alert, new.comment, new.created_by) == (
        "PumpPressureAlert",
        "why",
        None,
    )


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"alert": " "}, "alert: is required, use '*' for any."),
        ({"instrument_id": ""}, "instrument_id: is required, use '*' for any."),
        ({"identifier": " "}, "identifier: is required, use '*' for any."),
        ({"details": ""}, "details: is required, use '*' for any."),
        ({"comment": " "}, "comment: is required, state why the alerts are muted."),
        ({"alert": " * "}, "This would mute all alerts. Restrict at least one field."),
        ({"alert": "a" * 65}, "alert: String should have at most 64 characters"),
    ],
)
def test_new_suppression_rejects_invalid_suppression(
    overrides: dict[str, str], error: str
) -> None:
    """Suppressions with an empty or too long field or matching everything are rejected."""
    with pytest.raises(ValidationError) as e:
        parse(NewSuppression, **{**VALID, **overrides})

    assert e.value.errors == [error]
