"""Tests for the admin login."""

import pytest
from service.session_state import SessionStateKeys
from streamlit.testing.v1 import AppTest

from shared.keys import EnvVars

ADMIN_PASSWORD = "secret"  # noqa: S105  #Possible hardcoded password


def _app() -> None:
    # AppTest.from_function runs only the function body, so imports must live here
    from service.admin import show_admin_login

    show_admin_login()


def test_admin_login_hidden_without_password(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that no login is shown if no admin password is configured."""
    monkeypatch.delenv(EnvVars.ADMIN_PASSWORD, raising=False)

    at = AppTest.from_function(_app).run()

    assert len(at.sidebar.text_input) == 0


def test_admin_login_wrong_password(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that a wrong password does not grant admin."""
    monkeypatch.setenv(EnvVars.ADMIN_PASSWORD, ADMIN_PASSWORD)
    at = AppTest.from_function(_app).run()

    at.sidebar.text_input[0].input("wrong").run()

    assert SessionStateKeys.IS_ADMIN not in at.session_state
    assert at.sidebar.error[0].value == "Wrong password."


def test_admin_login_and_logout(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that the correct password grants admin, and logout revokes it."""
    monkeypatch.setenv(EnvVars.ADMIN_PASSWORD, ADMIN_PASSWORD)
    at = AppTest.from_function(_app).run()

    at.sidebar.text_input[0].input(ADMIN_PASSWORD).run()

    assert at.session_state[SessionStateKeys.IS_ADMIN]
    assert len(at.sidebar.text_input) == 0

    at.sidebar.button[0].click().run()

    assert not at.session_state[SessionStateKeys.IS_ADMIN]
    assert len(at.sidebar.text_input) == 1


@pytest.mark.parametrize(
    ("configured", "entered", "expected_admin"),
    [("secret", "geheimä", False), ("geheimä", "geheimä", True)],
)
def test_admin_login_with_non_ascii_password(
    monkeypatch: pytest.MonkeyPatch,
    configured: str,
    entered: str,
    *,
    expected_admin: bool,
) -> None:
    """Test that non-ASCII passwords are compared without error."""
    monkeypatch.setenv(EnvVars.ADMIN_PASSWORD, configured)
    at = AppTest.from_function(_app).run()

    at.sidebar.text_input[0].input(entered).run()

    assert not at.exception
    assert (
        at.session_state[SessionStateKeys.IS_ADMIN]
        if expected_admin
        else SessionStateKeys.IS_ADMIN not in at.session_state
    )
