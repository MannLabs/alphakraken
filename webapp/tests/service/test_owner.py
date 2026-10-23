"""Tests for the owner module."""

from unittest.mock import MagicMock, patch

import pandas as pd
from service.owner import add_owner_initials, show_owner_selectbox


def _user(initials: str) -> MagicMock:
    user = MagicMock(initials=initials)
    user.email = f"{initials.lower()}@b.de"
    return user


@patch("service.owner.get_all_users")
def test_add_owner_initials(mock_get_users: MagicMock) -> None:
    """Test that owner ids are replaced by initials, also for inactive and unknown owners."""
    mock_get_users.return_value = [_user("AB")]
    df = pd.DataFrame({"owner": ["ab@b.de", None, "unknown"]})
    original_df = df.copy()

    # when
    result = add_owner_initials(df)

    # then
    assert result["owner"].tolist() == ["AB", "", ""]
    pd.testing.assert_frame_equal(df, original_df)
    mock_get_users.assert_called_once_with(include_inactive=True)


@patch("service.owner.get_all_users")
def test_add_owner_initials_without_owner_column(mock_get_users: MagicMock) -> None:
    """Test that a frame without owners is returned as is."""
    df = pd.DataFrame({"name": ["a"]})

    # when
    result = add_owner_initials(df)

    # then
    assert result is df
    mock_get_users.assert_not_called()


@patch("service.owner.get_all_users", return_value=[])
def test_show_owner_selectbox_without_users(mock_get_users: MagicMock) -> None:  # noqa: ARG001
    """Test that nothing is shown without active users."""
    st_display = MagicMock()

    # when
    result = show_owner_selectbox(st_display)

    # then
    assert result is None
    st_display.selectbox.assert_not_called()


@patch("service.owner.get_all_users")
def test_show_owner_selectbox(mock_get_users: MagicMock) -> None:
    """Test that the active users are offered after the no-owner option."""
    mock_get_users.return_value = [_user("AB"), _user("CD")]
    st_display = MagicMock()

    # when
    result = show_owner_selectbox(st_display, "cd@b.de")

    # then
    assert result == st_display.selectbox.return_value
    mock_get_users.assert_called_once_with()
    kwargs = st_display.selectbox.call_args.kwargs
    assert kwargs["options"] == [None, "ab@b.de", "cd@b.de"]
    assert kwargs["index"] == 2
    assert [kwargs["format_func"](o) for o in kwargs["options"]] == [
        "(none)",
        "AB (ab@b.de)",
        "CD (cd@b.de)",
    ]


@patch("service.owner.get_all_users")
def test_show_owner_selectbox_with_inactive_default(mock_get_users: MagicMock) -> None:
    """Test that a default owner that is not active falls back to no owner."""
    mock_get_users.return_value = [_user("AB")]
    st_display = MagicMock()

    # when
    show_owner_selectbox(st_display, "inactive@b.de")

    # then
    assert st_display.selectbox.call_args.kwargs["index"] == 0
