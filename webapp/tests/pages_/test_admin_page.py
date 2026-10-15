"""Test the admin.py file."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from service.session_state import SessionStateKeys
from streamlit.testing.v1 import AppTest

from shared.db.models import UserStatus

PAGES_FOLDER = Path(__file__).parent / Path("../../pages_")


def _admin_app() -> AppTest:
    at = AppTest.from_file(f"{PAGES_FOLDER}/admin.py")
    at.session_state[SessionStateKeys.IS_ADMIN] = True
    return at


def _user(initials: str, status: str = UserStatus.ACTIVE) -> MagicMock:
    user = MagicMock(initials=initials, status=status)
    user.email = f"{initials.lower()}@b.de"
    return user


@patch("shared.db.interface.get_all_users")
def test_non_admin_sees_no_user_management(mock_get_users: MagicMock) -> None:
    """Test that a non-admin accessing the page directly gets no user management."""
    at = AppTest.from_file(f"{PAGES_FOLDER}/admin.py").run()

    assert not at.exception
    assert at.warning[0].value == "This page is only available to admins."
    assert not at.text_input
    mock_get_users.assert_not_called()


@patch("shared.db.interface.get_all_users", return_value=[])
def test_admin_without_users(mock_get_users: MagicMock) -> None:  # noqa: ARG001
    """Test that the page renders the add form and no edit section without users."""
    at = _admin_app().run()

    assert not at.exception
    assert not at.table
    assert len(at.text_input) == 2
    assert not at.selectbox


@patch("shared.db.interface.get_all_users")
def test_admin_shows_users(mock_get_users: MagicMock) -> None:
    """Test that the users are shown in the table and can be selected for editing."""
    mock_get_users.return_value = [
        _user("AB"),
        _user("CD", UserStatus.INACTIVE),
    ]

    at = _admin_app().run()

    assert not at.exception
    mock_get_users.assert_called_with(include_inactive=True)
    table = at.table[0].value
    assert table["initials"].tolist() == ["AB", "CD"]
    assert table["status"].tolist() == [UserStatus.ACTIVE, UserStatus.INACTIVE]
    assert at.selectbox[0].options == ["AB (ab@b.de, active)", "CD (cd@b.de, inactive)"]
    assert at.button[2].label == "Deactivate"


@patch("shared.db.interface.add_user")
@patch("shared.db.interface.get_all_users", return_value=[])
def test_admin_adds_user_with_cleaned_inputs(
    mock_get_users: MagicMock,  # noqa: ARG001
    mock_add_user: MagicMock,
) -> None:
    """Test that adding a user passes lowercased email and uppercased initials."""
    at = _admin_app().run()

    at.text_input[0].set_value(" A@B.de ")
    at.text_input[1].set_value("ab")
    at.button[0].click().run()

    assert not at.exception
    mock_add_user.assert_called_once_with(email="a@b.de", initials="AB")


@patch("shared.db.interface.update_user")
@patch("shared.db.interface.get_all_users")
def test_admin_updates_user(
    mock_get_users: MagicMock, mock_update_user: MagicMock
) -> None:
    """Test that saving an edited user calls update_user."""
    mock_get_users.return_value = [_user("AB")]
    at = _admin_app().run()

    at.text_input[2].set_value("xy")
    at.button[1].click().run()

    assert not at.exception
    mock_update_user.assert_called_once_with("ab@b.de", initials="XY")


@patch("shared.db.interface.set_user_status")
@patch("shared.db.interface.get_all_users")
def test_admin_reactivates_inactive_user(
    mock_get_users: MagicMock, mock_set_status: MagicMock
) -> None:
    """Test that the status button of an inactive user reactivates it."""
    mock_get_users.return_value = [_user("AB", UserStatus.INACTIVE)]
    at = _admin_app().run()

    at.button[2].click().run()

    assert not at.exception
    mock_set_status.assert_called_once_with("ab@b.de", UserStatus.ACTIVE)
