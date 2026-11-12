"""Tests for the domain.projects module."""

from typing import Any

import pytest

from shared.domain.errors import ValidationError
from shared.domain.inputs import parse
from shared.domain.projects import NewProject


def test_new_project_cleans_fields() -> None:
    """Strings are stripped, a blank description and owner become None."""
    new = parse(
        NewProject, project_id=" P123 ", name=" n ", description=" ", owner_email=""
    )

    assert new.model_dump() == {
        "project_id": "P123",
        "name": "n",
        "description": None,
        "owner_email": None,
    }


@pytest.mark.parametrize("project_id", ["A123", "ABCD", "1234A", "A" * 16, "_FALLBACK"])
def test_new_project_accepts_project_id(project_id: str) -> None:
    """Upper-case letters and digits with at least one letter, 4 to 16 long, or a special id."""
    assert parse(NewProject, project_id=project_id, name="n").project_id == project_id


@pytest.mark.parametrize(
    "project_id", ["", "A12", "A" * 17, "1234", "a123", "A-123", "A 123", "_OTHER"]
)
def test_new_project_rejects_project_id(project_id: str) -> None:
    """Too short, too long, all digits or other characters are rejected."""
    with pytest.raises(ValidationError) as e:
        parse(NewProject, project_id=project_id, name="n")

    assert e.value.errors == [
        f"project_id: Invalid project id '{project_id}'. Please choose a different one."
    ]


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"name": " "}, "name: String should have at least 1 character"),
        ({"name": "n" * 65}, "name: String should have at most 64 characters"),
        ({"description": "d" * 257}, "description: String should have at most 256"),
    ],
)
def test_new_project_rejects_invalid_fields(
    overrides: dict[str, Any], error: str
) -> None:
    """The name is required, name and description are limited in length."""
    with pytest.raises(ValidationError, match=error):
        parse(NewProject, **{"project_id": "P123", "name": "n", **overrides})
