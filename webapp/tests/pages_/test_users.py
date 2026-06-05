"""Tests for the users page."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from streamlit.testing.v1 import AppTest

PAGES_FOLDER = Path(__file__).parent / Path("../../pages_")


def _mock_user(initials: str, email: str, status: str = "active") -> MagicMock:
    user = MagicMock()
    user.initials = initials
    user.email = email
    user.slack_member_id = "U01ABC"
    user.status = status
    return user


@patch("shared.db.interface.get_all_users")
def test_users_display_table(mock_get_all_users: MagicMock) -> None:
    """Test that the table shows users correctly on the users page."""
    mock_get_all_users.return_value = [
        _mock_user("AB", "ab@example.com"),
        _mock_user("CD", "cd@example.com", status="inactive"),
    ]

    at = AppTest.from_file(f"{PAGES_FOLDER}/users.py").run(timeout=10)

    assert not at.exception
    table_data = at.table[0].value
    assert list(table_data["initials"]) == ["AB", "CD"]
    assert list(table_data["email"]) == ["ab@example.com", "cd@example.com"]
    assert list(table_data["status"]) == ["active", "inactive"]


@patch("shared.db.interface.get_all_users")
def test_users_non_admin_shows_warning(mock_get_all_users: MagicMock) -> None:
    """Without the admin query param, an admin-required warning is shown."""
    mock_get_all_users.return_value = []

    at = AppTest.from_file(f"{PAGES_FOLDER}/users.py").run(timeout=10)

    assert not at.exception
    assert any("admin privileges" in w.value for w in at.warning)


@patch("shared.db.interface.get_all_users")
def test_users_admin_no_warning(mock_get_all_users: MagicMock) -> None:
    """With admin=true, the admin-required warning is not shown."""
    mock_get_all_users.return_value = []

    at = AppTest.from_file(f"{PAGES_FOLDER}/users.py")
    at.query_params["admin"] = "true"
    at.run(timeout=10)

    assert not at.exception
    assert not any("admin privileges" in w.value for w in at.warning)
