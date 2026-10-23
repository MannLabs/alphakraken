"""Unit tests for recipient resolution from raw file names."""

from collections.abc import Generator
from unittest.mock import patch

import pytest

from monitoring.alerts.recipients import (
    INITIALS_PATTERN,
    extract_initials,
    get_recipients,
)

USER_IDS = {"MaSc": "U_MASC", "AnSc": "U_ANSC"}


@pytest.fixture(autouse=True)
def _user_ids() -> Generator[None, None, None]:
    with patch.dict(
        "monitoring.alerts.config.INSTRUMENT_USER_SLACK_IDS", USER_IDS, clear=True
    ):
        yield


def test_initials_pattern_matches_adjacent_tokens() -> None:
    """Adjacent tokens sharing one underscore both match (overlapping boundary)."""
    # when
    tokens = [m.group(1) for m in INITIALS_PATTERN.finditer("x_SA_AnSc_y.raw")]
    # then
    assert tokens == ["SA", "AnSc"]


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("20260101_SA_AnSc_sample.raw", "AnSc"),
        ("20260101_MaSc_AnSc_sample.raw", "MaSc"),
        ("20260101_UNK_sample.raw", None),
        ("MaSc.raw", None),
        ("", None),
        (None, None),
    ],
)
def test_extract_initials_returns_first_mapped_token(
    name: str | None, expected: str | None
) -> None:
    """Only tokens present in the user map count as initials."""
    # when / then
    assert extract_initials(name) == expected


@patch("monitoring.alerts.recipients.SPECIAL_ALERT_SLACK_ID", None)
def test_get_recipients_returns_mapped_user() -> None:
    """Without a special ID, only the mapped user is returned."""
    # when / then
    assert get_recipients("x_MaSc_a.raw") == ["U_MASC"]


@patch("monitoring.alerts.recipients.SPECIAL_ALERT_SLACK_ID", "U_SUP")
def test_get_recipients_includes_special_id_when_configured() -> None:
    """When SPECIAL_ALERT_SLACK_ID is set, it is CC'd."""
    # when / then
    assert get_recipients("x_MaSc_a.raw") == ["U_MASC", "U_SUP"]


@patch("monitoring.alerts.recipients.SPECIAL_ALERT_SLACK_ID", "U_MASC")
def test_get_recipients_deduplicates_when_user_id_equals_special_id() -> None:
    """If the mapped user IS the special ID, it is returned once."""
    # when / then
    assert get_recipients("x_MaSc_a.raw") == ["U_MASC"]


@pytest.mark.parametrize("file_name", [None, "x_UNK_a.raw"])
@patch("monitoring.alerts.recipients.SPECIAL_ALERT_SLACK_ID", "U_SUP")
def test_get_recipients_returns_empty_when_no_user_found(
    file_name: str | None,
) -> None:
    """No mapped user means no recipients, not even the special ID."""
    # when / then
    assert get_recipients(file_name) == []
