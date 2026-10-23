"""Test the admin.py file."""

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest
from service.session_state import SessionStateKeys
from streamlit.testing.v1 import AppTest

from shared.db.models import AlertSuppression, UserStatus

PAGES_FOLDER = Path(__file__).parent / Path("../../pages_")


@pytest.fixture(autouse=True)
def _no_alert_suppressions():  # noqa: ANN202
    # the alert suppressions section would otherwise query the DB
    with patch("shared.db.interface.get_alert_suppressions", return_value=[]):
        yield


def _admin_app() -> AppTest:
    at = AppTest.from_file(f"{PAGES_FOLDER}/admin.py")
    at.session_state[SessionStateKeys.IS_ADMIN] = True
    return at


def _user(initials: str, status: str = UserStatus.ACTIVE) -> MagicMock:
    user = MagicMock(initials=initials, status=status, messenger_id=None)
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
    assert sum(t.label == "Initials*" for t in at.text_input) == 1
    assert "User" not in [s.label for s in at.selectbox]


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
    assert at.button[3].label == "Deactivate"


@patch("shared.db.interface.add_user")
@patch("shared.db.interface.get_all_users", return_value=[])
def test_admin_adds_user_with_cleaned_inputs(
    mock_get_users: MagicMock,  # noqa: ARG001
    mock_add_user: MagicMock,
) -> None:
    """Test that adding a user passes the email and the stripped initials with their case kept."""
    at = _admin_app().run()

    at.text_input[0].set_value(" A@B.de ")
    at.text_input[1].set_value(" Ab ")
    at.text_input[2].set_value(" U123 ")
    at.button[0].click().run()

    assert not at.exception
    mock_add_user.assert_called_once_with(
        email=" A@B.de ", initials="Ab", messenger_id="U123"
    )


@patch("shared.db.interface.update_user")
@patch("shared.db.interface.get_all_users")
def test_admin_updates_user(
    mock_get_users: MagicMock, mock_update_user: MagicMock
) -> None:
    """Test that saving an edited user calls update_user."""
    mock_get_users.return_value = [_user("AB")]
    at = _admin_app().run()

    at.text_input[3].set_value("xY")
    at.text_input[4].set_value("U123")
    at.button[2].click().run()

    assert not at.exception
    mock_update_user.assert_called_once_with(
        "ab@b.de", initials="xY", messenger_id="U123"
    )


@patch("shared.db.interface.set_user_status")
@patch("shared.db.interface.get_all_users")
def test_admin_reactivates_inactive_user(
    mock_get_users: MagicMock, mock_set_status: MagicMock
) -> None:
    """Test that the status button of an inactive user reactivates it."""
    mock_get_users.return_value = [_user("AB", UserStatus.INACTIVE)]
    at = _admin_app().run()

    at.button[3].click().run()

    assert not at.exception
    mock_set_status.assert_called_once_with("ab@b.de", UserStatus.ACTIVE)


@patch("shared.db.interface.add_user")
@patch("shared.db.interface.get_all_users", return_value=[])
def test_admin_bulk_adds_users_and_reports_failures(
    mock_get_users: MagicMock,  # noqa: ARG001
    mock_add_user: MagicMock,
) -> None:
    """Test that valid rows are added and failing rows are reported with their line."""
    mock_add_user.side_effect = [None, ValueError("initials 'CD' already exists.")]
    at = _admin_app().run()

    at.text_area[0].set_value("a@b.de,Ab,U123\n\nC@D.de,CD\nbad row")
    at.button[1].click().run()

    assert not at.exception
    assert mock_add_user.call_args_list == [
        call(email="a@b.de", initials="Ab", messenger_id="U123"),
        call(email="C@D.de", initials="CD", messenger_id=None),
    ]
    assert at.success[0].value == "Added 1 users: Ab"
    assert at.error[0].value == (
        "Failed to add 2 users:\n"
        "- line 3: initials 'CD' already exists.\n"
        "- line 4: expected 'e-mail,initials[,messenger id]', got 'bad row'."
    )


@patch("shared.db.interface.add_user")
@patch("shared.db.interface.get_all_users", return_value=[])
def test_admin_bulk_add_report_is_shown_once(
    mock_get_users: MagicMock,  # noqa: ARG001
    mock_add_user: MagicMock,  # noqa: ARG001
) -> None:
    """Test that the report disappears on the next interaction."""
    at = _admin_app().run()
    at.text_area[0].set_value("a@b.de,ab")
    at.button[1].click().run()
    assert at.success

    at.run()

    assert not at.success


@patch("shared.db.interface.get_all_users", return_value=[])
@patch("shared.db.interface.get_alert_suppressions", return_value=[])
def test_admin_page_should_render_without_suppressions(
    mock_get: MagicMock,  # noqa: ARG001
    mock_get_users: MagicMock,  # noqa: ARG001
) -> None:
    """The page renders an empty state and the add form."""
    # when
    at = _admin_app().run(timeout=30)

    # then
    assert not at.exception
    assert any("No alert suppressions" in i.value for i in at.info)
    assert at.button(key="FormSubmitter:add_suppression_form-Add suppression")


@patch("shared.db.interface.get_all_users", return_value=[])
@patch("shared.db.interface.get_alert_suppressions")
def test_admin_page_should_show_suppressions(
    mock_get: MagicMock,
    mock_get_users: MagicMock,  # noqa: ARG001
) -> None:
    """Existing suppressions are shown in a table, each with a delete button."""
    # given
    mock_get.return_value = [
        AlertSuppression(
            id="0123456789abcdef01234567",
            error_class="PumpPressureAlert",
            instrument_id="astral1",
            comment="maintenance",
        )
    ]

    # when
    at = _admin_app().run(timeout=30)

    # then
    assert not at.exception
    df = at.dataframe[0].value
    assert df["alert class"].tolist() == ["PumpPressureAlert"]
    assert df["instrument"].tolist() == ["astral1"]
    assert at.button(key="delete_suppression_0123456789abcdef01234567")
