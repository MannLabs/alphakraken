"""Unit tests for recipient resolution from raw file names."""

from unittest.mock import MagicMock, patch

import pytest

from monitoring.alerts.recipients import (
    INITIALS_PATTERN,
    extract_initials,
    get_messenger_ids_by_initials,
    get_recipients,
)

MESSENGER_IDS = {"MaSc": "U_MASC", "AnSc": "U_ANSC"}


def test_initials_pattern_matches_underscored_tokens_only() -> None:
    """The initials regex matches only underscored 2-8 letter tokens."""
    assert INITIALS_PATTERN.search("_MaSc_") is not None
    assert INITIALS_PATTERN.search("xxx_AB_yyy") is not None
    # Bare token (no surrounding underscores) must NOT match
    assert INITIALS_PATTERN.search("MaScfoo") is None
    # 1-char tokens excluded by {2,8}
    assert INITIALS_PATTERN.search("_A_") is None
    # 9-char tokens excluded
    assert INITIALS_PATTERN.search("_ABCDEFGHI_") is None


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
def test_extract_initials_returns_first_known_token(
    name: str | None, expected: str | None
) -> None:
    """Only tokens that are known initials count."""
    # when / then
    assert extract_initials(name, {"MaSc", "AnSc"}) == expected


@patch("monitoring.alerts.recipients.get_project_owner", return_value=None)
def test_get_recipients_returns_messenger_id_of_user(
    mock_get_project_owner: MagicMock,
) -> None:
    """Without a project owner, the first user named in the file is the recipient."""
    # when / then
    assert get_recipients("x_MaSc_a.raw", MESSENGER_IDS) == ["U_MASC"]
    mock_get_project_owner.assert_called_once_with("x_MaSc_a.raw")


@patch("monitoring.alerts.recipients.get_project_owner")
def test_get_recipients_prefers_the_project_owner(
    mock_get_project_owner: MagicMock,
) -> None:
    """The owner of the file's project is the recipient, not the user named in the file."""
    # given
    mock_get_project_owner.return_value = MagicMock(messenger_id="U_OWNER")

    # when / then
    assert get_recipients("x_MaSc_a.raw", MESSENGER_IDS) == ["U_OWNER"]


@patch("monitoring.alerts.recipients.get_project_owner")
def test_get_recipients_falls_back_to_initials_when_owner_has_no_messenger_id(
    mock_get_project_owner: MagicMock,
) -> None:
    """An owner without a messenger id does not take precedence."""
    # given
    mock_get_project_owner.return_value = MagicMock(messenger_id=None)

    # when / then
    assert get_recipients("x_MaSc_a.raw", MESSENGER_IDS) == ["U_MASC"]


@pytest.mark.parametrize("file_name", [None, "x_UNK_a.raw"])
@patch("monitoring.alerts.recipients.get_project_owner", return_value=None)
def test_get_recipients_returns_empty_when_no_user_found(
    mock_get_project_owner: MagicMock,  # noqa: ARG001
    file_name: str | None,
) -> None:
    """No project owner and no user with a messenger id means no recipients."""
    # when / then
    assert get_recipients(file_name, MESSENGER_IDS) == []


@patch("monitoring.alerts.recipients.get_all_users")
def test_get_messenger_ids_by_initials_from_active_users_with_an_id(
    mock_get_all_users: MagicMock,
) -> None:
    """Only active users with a messenger id are known."""
    # given
    mock_get_all_users.return_value = [
        MagicMock(initials="MaSc", messenger_id="U_MASC"),
        MagicMock(initials="AnSc", messenger_id=None),
    ]

    # when / then
    assert get_messenger_ids_by_initials() == {"MaSc": "U_MASC"}
    mock_get_all_users.assert_called_with()
