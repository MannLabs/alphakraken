"""Optional password-based admin login."""

import hmac
import os

import streamlit as st
from service.session_state import SessionStateKeys, get_session_state, set_session_state

from shared.keys import EnvVars


def is_admin() -> bool:
    """Whether the current session is logged in as admin."""
    return get_session_state(SessionStateKeys.IS_ADMIN, default=False)


def show_admin_login() -> None:
    """Show admin login/logout in the sidebar, if an admin password is configured."""
    expected_password = os.environ.get(EnvVars.ADMIN_PASSWORD)
    if not expected_password:
        return

    with st.sidebar:
        st.write("#### Admin login")
        if is_admin():
            if st.button("Log out"):
                set_session_state(SessionStateKeys.IS_ADMIN, value=False)
                st.rerun()
        else:
            password = st.text_input("Password", type="password")
            if password:
                if hmac.compare_digest(password.encode(), expected_password.encode()):
                    set_session_state(SessionStateKeys.IS_ADMIN, value=True)
                    st.rerun()
                st.error("Wrong password.")
