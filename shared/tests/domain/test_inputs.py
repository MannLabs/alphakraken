"""Tests for the domain.inputs module."""

from typing import Annotated, Any, Self

import pytest
from pydantic import (
    BaseModel,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

from shared.domain.errors import ValidationError
from shared.domain.inputs import empty_to_none, max_length, parse


class _Input(BaseModel):
    name: str
    size: int = 1

    @field_validator("name")
    @classmethod
    def _not_reserved(cls, value: str) -> str:
        if value == "reserved":
            raise ValueError("is reserved.")
        return value

    @model_validator(mode="after")
    def _size_fits_name(self) -> Self:
        if self.size > len(self.name):
            raise ValueError("Size exceeds the name.")
        return self


def test_parse_returns_input_type() -> None:
    """Valid fields yield the input type."""
    assert parse(_Input, name="abc", size=2) == _Input(name="abc", size=2)


def test_parse_reports_every_invalid_field() -> None:
    """Each invalid field gets one entry, prefixed with its name."""
    with pytest.raises(ValidationError) as e:
        parse(_Input, name="reserved", size="big")

    assert e.value.errors == [
        "name: is reserved.",
        "size: Input should be a valid integer, unable to parse string as an integer",
    ]


def test_parse_reports_missing_field() -> None:
    """A missing required field is an error like an invalid one."""
    with pytest.raises(ValidationError, match="name: Field required"):
        parse(_Input)


def test_parse_reports_cross_field_rule_without_prefix() -> None:
    """A rule spanning several fields is reported with its bare message."""
    with pytest.raises(ValidationError) as e:
        parse(_Input, name="abc", size=4)

    assert e.value.errors == ["Size exceeds the name."]


@pytest.mark.parametrize(
    ("value", "expected"),
    [("", None), ("  ", None), (" a ", " a "), (None, None), (0, 0)],
)
def test_empty_to_none(value: Any, expected: Any) -> None:
    """Only empty and blank strings become None."""
    assert empty_to_none(value) == expected


class _Limited(BaseModel):
    plain: str | None = Field(default=None, max_length=8)
    constrained: Annotated[str, StringConstraints(to_lower=True, max_length=16)]


@pytest.mark.parametrize(("field", "expected"), [("plain", 8), ("constrained", 16)])
def test_max_length(field: str, expected: int) -> None:
    """The declared maximum length is found for both ways of declaring it."""
    assert max_length(_Limited, field) == expected
