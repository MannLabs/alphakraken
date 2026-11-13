"""Tests for the domain.projects module."""

from typing import Any

import pytest

from shared.domain.errors import ValidationError
from shared.domain.inputs import max_length, parse
from shared.domain.projects import NewAssignment, NewProject


def test_new_project_cleans_fields() -> None:
    """Strings are stripped, a blank description and owner become None, the owner is lower-cased."""
    new = parse(
        NewProject, project_id=" P123 ", name=" n ", description=" ", owner_email=""
    )
    owned = parse(NewProject, project_id="P123", name="n", owner_email=" Jane@B.de ")

    assert new.model_dump() == {
        "project_id": "P123",
        "name": "n",
        "description": None,
        "owner_email": None,
    }
    assert owned.owner_email == "jane@b.de"


@pytest.mark.parametrize("project_id", ["A123", "ABCD", "1234A", "A" * 16, "_FALLBACK"])
def test_new_project_accepts_project_id(project_id: str) -> None:
    """Upper-case letters and digits with at least one letter, 4 to 16 long, or a special id."""
    assert parse(NewProject, project_id=project_id, name="n").project_id == project_id


@pytest.mark.parametrize(
    "project_id", ["A12", "1234", "a123", "A-123", "A 123", "_OTHER"]
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
        ({"name": " "}, "name: is required."),
        ({"project_id": ""}, "project_id: is required."),
        ({"name": None}, "name: is required."),
        ({"name": "n" * 65}, "name: String should have at most 64 characters"),
        (
            {"project_id": "A" * 17},
            "project_id: String should have at most 16 characters",
        ),
        ({"description": "d" * 513}, "description: String should have at most 512"),
    ],
)
def test_new_project_rejects_invalid_fields(
    overrides: dict[str, Any], error: str
) -> None:
    """The name is required, name and description are limited in length."""
    with pytest.raises(ValidationError, match=error):
        parse(NewProject, **{"project_id": "P123", "name": "n", **overrides})


def test_new_project_declares_project_id_limit() -> None:
    """The forms can read the limit of the project id."""
    assert max_length(NewProject, "project_id") == 16


ASSIGNMENT: dict[str, Any] = {"project_id": "P123", "settings_id": "fallback_v7"}


def test_new_assignment_defaults_to_all_instruments_and_files() -> None:
    """Without scopes and filters, the assignment applies to everything."""
    new = parse(NewAssignment, **ASSIGNMENT)

    assert new.model_dump() == {
        **ASSIGNMENT,
        "scopes": ["*"],
        "excluded_scopes": [],
        "raw_file_id_filter": [],
        "raw_file_id_exclude_filter": [],
    }


@pytest.mark.parametrize(
    ("scopes", "expected"),
    [
        (["test1", "*"], ["*"]),
        (["test1", "thermo"], ["test1", "thermo"]),
    ],
)
def test_new_assignment_normalizes_scopes(
    scopes: list[str], expected: list[str]
) -> None:
    """The default scope absorbs the scopes next to it."""
    assert parse(NewAssignment, **ASSIGNMENT, scopes=scopes).scopes == expected


def test_new_assignment_strips_entries() -> None:
    """Entries are stripped."""
    new = parse(NewAssignment, **ASSIGNMENT, raw_file_id_filter=[" hela "])

    assert new.raw_file_id_filter == ["hela"]


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        (
            {"excluded_scopes": ["*"]},
            "The scope '*' cannot be excluded, the assignment would never apply.",
        ),
        (
            {"scopes": ["test1", "thermo"], "excluded_scopes": ["thermo", "test2"]},
            "Scopes ['thermo'] are both included and excluded, they would never apply.",
        ),
        (
            {"scopes": []},
            "scopes: At least one scope is required, use '*' for all instruments.",
        ),
        ({"scopes": "x*y"}, "scopes: Input should be a valid list"),
        (
            {
                "raw_file_id_filter": ["HeLa_QC", "plasma"],
                "raw_file_id_exclude_filter": ["hela"],
            },
            "Files matching ['HeLa_QC'] always match an exclude filter, they would never apply.",
        ),
        (
            {"raw_file_id_filter": ["a", " "]},
            "raw_file_id_filter.1: String should have at least 1 character",
        ),
        (
            {"raw_file_id_exclude_filter": ["x" * 129]},
            "raw_file_id_exclude_filter.0: String should have at most 128 characters",
        ),
        ({"scopes": ["s" * 65]}, "scopes.0: String should have at most 64 characters"),
    ],
)
def test_new_assignment_rejects_invalid_fields(
    overrides: dict[str, Any], error: str
) -> None:
    """No scopes, exclusions that leave nothing and empty or too long entries are rejected."""
    with pytest.raises(ValidationError) as e:
        parse(NewAssignment, **ASSIGNMENT, **overrides)

    assert e.value.errors == [error]
