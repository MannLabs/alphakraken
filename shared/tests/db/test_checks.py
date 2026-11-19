"""Tests for the checks shared by the write functions."""

from shared.db._checks import inactive_errors


def test_inactive_errors_of_active_record() -> None:
    """An active record is no error."""
    assert inactive_errors("Project 'P1'", "active", "active") == []


def test_inactive_errors_names_record_and_status() -> None:
    """A record that is not active is named with its status."""
    assert inactive_errors("Project 'P1'", "deleted", "active") == [
        "Project 'P1' is not active (status 'deleted')."
    ]
