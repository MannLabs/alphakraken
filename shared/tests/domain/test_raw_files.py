"""Tests for the domain.raw_files module."""

from shared.domain.inputs import parse
from shared.domain.raw_files import STATUS_DETAILS_MAX_LENGTH, RawFileUpdate


def test_raw_file_update_truncates_too_long_status_details() -> None:
    """Too long status details are cut to the limit and end in `...`, so that the status is still written."""
    new = parse(RawFileUpdate, status_details="x" * (STATUS_DETAILS_MAX_LENGTH + 1))

    assert new.status_details == "x" * (STATUS_DETAILS_MAX_LENGTH - 3) + "..."


def test_raw_file_update_keeps_status_details_within_the_limit() -> None:
    """Status details that fit are kept as they are."""
    details = "x" * STATUS_DETAILS_MAX_LENGTH

    assert parse(RawFileUpdate, status_details=details).status_details == details
