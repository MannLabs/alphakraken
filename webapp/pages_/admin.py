"""Admin page, only registered in the navigation for admins."""

import pandas as pd
import streamlit as st
from service.admin import is_admin
from service.query_params import get_all_query_params
from service.session_state import SessionStateKeys, set_session_state
from service.user_input import clean_initials, parse_user_row
from service.utils import (
    DISABLE_WRITE,
    _log,
    flush_pending_toasts,
    show_error_toast,
    show_success_toast,
)

from shared.db.interface import add_user, get_all_users, set_user_status, update_user
from shared.db.models import UserStatus

_log(f"loading {__file__} {get_all_query_params()}")

st.set_page_config(page_title="AlphaKraken: admin", layout="wide")

flush_pending_toasts()

st.markdown("# Admin")

# the page is not registered for non-admins, this guards against direct access
if not is_admin():
    st.warning("This page is only available to admins.")
    st.stop()


# ########################################### USERS

st.markdown("## Users")

users = get_all_users(include_inactive=True)

if users:
    st.table(
        pd.DataFrame(
            [
                {"initials": u.initials, "email": u.email, "status": u.status}
                for u in users
            ]
        )
    )
else:
    st.info(
        "No users yet. Once there are users, they can be set as owner of new projects and settings."
    )

c1, _ = st.columns([0.5, 0.5])

c1.markdown("### Add user")

with c1.form("add_user_form"):
    new_email = st.text_input("Email*", max_chars=128)
    new_initials = st.text_input("Initials*", max_chars=8)
    add_submit = st.form_submit_button("Add user", disabled=DISABLE_WRITE)

if add_submit:
    try:
        add_user(email=new_email, initials=clean_initials(new_initials))
    except Exception as e:  # noqa: BLE001
        show_error_toast(str(e))
    else:
        show_success_toast(f"Added user '{clean_initials(new_initials)}'.")

# cleared, as the report lists the failed rows
with c1.form("bulk_add_users_form"):
    bulk_rows = st.text_area(
        "Bulk add users",
        placeholder="jane.doe@example.com,JD\njohn.roe@example.com,JR",
        help="One user per row as `e-mail,initials` (or tab-separated, e.g. pasted from a spreadsheet). Valid rows are added even if others fail.",
    )
    bulk_submit = st.form_submit_button("Add users", disabled=DISABLE_WRITE)

if bulk_submit:
    added, failures = [], []
    for line_number, row in enumerate(bulk_rows.splitlines(), start=1):
        if not row.strip():
            continue
        try:
            email, initials = parse_user_row(row)
            add_user(email=email, initials=initials)
        except Exception as e:  # noqa: BLE001
            failures.append(f"line {line_number}: {e}")
        else:
            added.append(initials)
    # shown after the rerun, which refreshes the users table
    set_session_state(SessionStateKeys.BULK_ADD_USERS_REPORT, (added, failures))
    st.rerun()

if report := st.session_state.pop(SessionStateKeys.BULK_ADD_USERS_REPORT, None):
    added, failures = report
    if added:
        c1.success(f"Added {len(added)} users: {', '.join(added)}")
    if failures:
        c1.error(
            f"Failed to add {len(failures)} users:\n"
            + "\n".join(f"- {f}" for f in failures)
        )

if users:
    c1.markdown("### Edit user")

    users_by_email = {u.email: u for u in users}
    selected_email = c1.selectbox(
        "User",
        options=list(users_by_email),
        format_func=lambda email: f"{users_by_email[email].initials} ({email}, {users_by_email[email].status})",
    )
    user = users_by_email[selected_email]

    # keyed by user, so that the input is prefilled anew when another user is selected
    initials = c1.text_input(
        "Initials*", value=user.initials, max_chars=8, key=f"initials_{selected_email}"
    )
    c1.caption(
        "The email can't be changed: add a new user and deactivate this one instead."
    )

    col_save, col_status = c1.columns([0.5, 0.5])
    if col_save.button("Save", disabled=DISABLE_WRITE):
        try:
            update_user(selected_email, initials=clean_initials(initials))
        except Exception as e:  # noqa: BLE001
            show_error_toast(str(e))
        else:
            show_success_toast(f"Updated user '{clean_initials(initials)}'.")

    is_active = user.status == UserStatus.ACTIVE
    if col_status.button(
        "Deactivate" if is_active else "Reactivate",
        disabled=DISABLE_WRITE,
        help="Inactive users can't be selected as owner, but stay owner of existing projects and settings.",
    ):
        set_user_status(
            selected_email, UserStatus.INACTIVE if is_active else UserStatus.ACTIVE
        )
        show_success_toast(
            f"{'Deactivated' if is_active else 'Reactivated'} user '{user.initials}'."
        )
