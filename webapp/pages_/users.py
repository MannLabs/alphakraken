"""User management page (admin only)."""

import re

import pandas as pd
import streamlit as st
from service.components import show_sandbox_message
from service.query_params import QueryParams, get_all_query_params, is_query_param_true
from service.utils import (
    DISABLE_WRITE,
    _log,
    empty_to_none,
    flush_pending_toasts,
    show_error_toast,
    show_success_toast,
)

from shared.db.interface import (
    add_user,
    deactivate_user,
    get_all_users,
    update_user,
)
from shared.db.models import EMAIL_REGEX, UserStatus
from shared.validation import check_for_malicious_content

_log(f"loading {__file__} {get_all_query_params()}")

# ########################################### PAGE HEADER

st.set_page_config(page_title="AlphaKraken: users", layout="wide")

flush_pending_toasts()
show_sandbox_message()

st.markdown("# Users")

is_admin = is_query_param_true(QueryParams.ADMIN)
disable_write = DISABLE_WRITE or not is_admin

if not is_admin:
    st.warning(
        "User management requires admin privileges. The view below is read-only."
    )

# ########################################### LOGIC

users = get_all_users(include_inactive=True)

# ########################################### DISPLAY

st.markdown("## Current users")

users_df = pd.DataFrame(
    [
        {
            "initials": u.initials,
            "email": u.email,
            "slack_member_id": u.slack_member_id or "",
            "status": u.status,
        }
        for u in users
    ]
)

if users_df.empty:
    st.info("No users yet. Create one below.")
else:
    st.table(
        users_df.style.apply(
            lambda row: [
                "color: lightgray"
                if row["status"] == UserStatus.INACTIVE
                else "background-color: white"
            ]
            * len(row),
            axis=1,
        )
    )

# ########################################### CREATE USER

c1, _ = st.columns([0.5, 0.5])

c1.markdown("## Add new user")

form_items = {
    "email": {
        "label": "E-Mail*",
        "max_chars": 128,
        "placeholder": "e.g. jane.doe@example.com",
        "help": "Unique identifier of the user.",
    },
    "initials": {
        "label": "Initials*",
        "max_chars": 16,
        "placeholder": "e.g. JaDo",
        "help": "Short, unique initials of the user as used in raw file names.",
    },
    "slack_member_id": {
        "label": "Slack Member ID",
        "max_chars": 32,
        "placeholder": "(optional) e.g. U01ABC2DEF",
        "help": "Slack member ID of the user.",
    },
}

with c1.form("create_user_form"):
    email = st.text_input(**form_items["email"])
    initials = st.text_input(**form_items["initials"])
    slack_member_id = st.text_input(**form_items["slack_member_id"])

    st.write(r"\* Required fields")
    create_submit = st.form_submit_button(
        "Create user",
        disabled=disable_write,
        help="Temporarily disabled." if DISABLE_WRITE else "",
    )


def _validate_user_input(
    email: str | None, initials: str | None, slack_member_id: str | None
) -> list[str]:
    """Validate user input, returning a list of error messages."""
    errors = []
    if not email or not re.match(EMAIL_REGEX, email):
        errors.append(f"Invalid e-mail '{email}'.")
    if not initials:
        errors.append("Initials are required.")
    else:
        errors.extend(check_for_malicious_content(initials))
    if slack_member_id:
        errors.extend(check_for_malicious_content(slack_member_id))
    return errors


if create_submit:
    email_clean = empty_to_none(email)
    initials_clean = empty_to_none(initials)
    slack_clean = empty_to_none(slack_member_id)
    try:
        validation_errors = _validate_user_input(
            email_clean, initials_clean, slack_clean
        )
        if validation_errors:
            errors_str = "\n- ".join(validation_errors)
            raise ValueError(f"Input validation error:\n- {errors_str}")  # noqa: TRY301

        add_user(
            email=email_clean,
            initials=initials_clean,
            slack_member_id=slack_clean,
        )
    except Exception as e:  # noqa: BLE001
        show_error_toast(str(e))
    else:
        show_success_toast(f"Added new user '{email_clean}' to the database.")

# ########################################### EDIT / DEACTIVATE

c1.markdown("## Edit users")

active_users = [u for u in users if u.status == UserStatus.ACTIVE]

if not active_users:
    c1.info("No active users to edit.")
else:
    with c1.expander("Show users to edit .."):
        for u in active_users:
            st.markdown(f"{u.email}` [{u.initials}]")
            col_initials, col_slack = st.columns([0.5, 0.5])
            new_initials = col_initials.text_input(
                "Initials",
                value=u.initials,
                max_chars=16,
                key=f"edit_initials_{u.email}",
            )
            new_slack = col_slack.text_input(
                "Slack Member ID",
                value=u.slack_member_id or "",
                max_chars=32,
                key=f"edit_slack_{u.email}",
            )
            col_save, col_deactivate, _ = st.columns([0.2, 0.2, 0.6])
            if col_save.button(
                "Save",
                key=f"save_user_{u.email}",
                disabled=disable_write,
                icon=":material/save:",
            ):
                try:
                    errors = check_for_malicious_content(new_initials)
                    if new_slack:
                        errors.extend(check_for_malicious_content(new_slack))
                    if not new_initials:
                        errors.append("Initials are required.")
                    if errors:
                        errors_str = "\n- ".join(errors)
                        raise ValueError(  # noqa: TRY301
                            f"Input validation error:\n- {errors_str}"
                        )
                    update_user(
                        u.email,
                        initials=new_initials,
                        slack_member_id=empty_to_none(new_slack),
                    )
                    show_success_toast(f"Updated user '{u.email}'.")
                except Exception as e:  # noqa: BLE001
                    show_error_toast(str(e))
            if col_deactivate.button(
                "Deactivate",
                key=f"deactivate_user_{u.email}",
                disabled=disable_write,
                help="Soft-delete this user. They remain owner of existing projects/settings.",
                icon=":material/person_off:",
            ):
                try:
                    deactivate_user(u.email)
                    show_success_toast(f"Deactivated user '{u.email}'.")
                except Exception as e:  # noqa: BLE001
                    show_error_toast(str(e))
