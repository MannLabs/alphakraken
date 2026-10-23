"""Admin page, only registered in the navigation for admins."""

import pandas as pd
import streamlit as st
from pages_.impl.admin_utils import (
    end_of_day,
    get_suppressions_df,
    validate_suppression,
)
from service.admin import is_admin
from service.query_params import get_all_query_params
from service.session_state import SessionStateKeys, set_session_state
from service.timezone import display_now, to_display_timezone
from service.user_input import clean_initials, clean_messenger_id, parse_user_row
from service.utils import (
    DISABLE_WRITE,
    _log,
    empty_to_none,
    flush_pending_toasts,
    show_error_toast,
    show_success_toast,
)

from shared.db.interface import (
    add_alert_suppression,
    add_user,
    delete_alert_suppression,
    get_alert_suppressions,
    get_all_users,
    set_user_status,
    update_user,
)
from shared.db.models import UserStatus
from shared.keys import ALERT_SUPPRESSION_WILDCARD
from shared.yamlsettings import YAMLSETTINGS, YamlKeys

_INSTRUMENT_IDS = list(YAMLSETTINGS.get(YamlKeys.INSTRUMENTS, {}).keys())

_MESSENGER_ID_HELP = (
    "Id of the user in the messenger (e.g. Slack member ID), to notify the user directly "
    "about alerts concerning raw files with their initials."
)

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
                {
                    "initials": u.initials,
                    "email": u.email,
                    "messenger id": u.messenger_id,
                    "status": u.status,
                }
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
    new_messenger_id = st.text_input(
        "Messenger id", max_chars=64, help=_MESSENGER_ID_HELP
    )
    add_submit = st.form_submit_button("Add user", disabled=DISABLE_WRITE)

if add_submit:
    try:
        add_user(
            email=new_email,
            initials=clean_initials(new_initials),
            messenger_id=clean_messenger_id(new_messenger_id),
        )
    except Exception as e:  # noqa: BLE001
        show_error_toast(str(e))
    else:
        show_success_toast(f"Added user '{clean_initials(new_initials)}'.")

# cleared, as the report lists the failed rows
with c1.form("bulk_add_users_form"):
    bulk_rows = st.text_area(
        "Bulk add users",
        placeholder="jane.doe@example.com,JD,U0123ABCD\njohn.roe@example.com,JR",
        help="One user per row as `e-mail,initials[,messenger id]` (or tab-separated, e.g. pasted from a spreadsheet). Valid rows are added even if others fail.",
    )
    bulk_submit = st.form_submit_button("Add users", disabled=DISABLE_WRITE)

if bulk_submit:
    added, failures = [], []
    for line_number, row in enumerate(bulk_rows.splitlines(), start=1):
        if not row.strip():
            continue
        try:
            email, initials, messenger_id = parse_user_row(row)
            add_user(email=email, initials=initials, messenger_id=messenger_id)
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
    messenger_id = c1.text_input(
        "Messenger id",
        value=user.messenger_id or "",
        max_chars=64,
        key=f"messenger_id_{selected_email}",
        help=_MESSENGER_ID_HELP,
    )
    c1.caption(
        "The email can't be changed: add a new user and deactivate this one instead."
    )

    col_save, col_status = c1.columns([0.5, 0.5])
    if col_save.button("Save", disabled=DISABLE_WRITE):
        try:
            update_user(
                selected_email,
                initials=clean_initials(initials),
                messenger_id=clean_messenger_id(messenger_id),
            )
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


# ########################################### ALERT SUPPRESSIONS

st.markdown("## Alert suppressions")

st.info(
    f"""
    Alerts matching a suppression are not sent. A suppression matches an alert if all of its fields match,
    `{ALERT_SUPPRESSION_WILDCARD}` matches anything:
    - **Alert**: class name of the alert, e.g. `PumpPressureHighAlert` (cf. `monitoring/alerts`).
    - **Instrument**: the instrument the alert relates to.
    - **Identifier**: the exact identifier shown in the alert, e.g. an instrument or raw file id.
    - **Details**: a part of the alert text.

    Expired suppressions no longer apply. Changes take effect with the next monitoring check.
    """,
    icon="ℹ️",  # noqa: RUF001
)

suppressions = get_alert_suppressions()

if not suppressions:
    st.info("No alert suppressions.")
else:
    st.dataframe(
        to_display_timezone(get_suppressions_df(suppressions)),
        hide_index=True,
        use_container_width=True,
    )

    with st.expander("Delete suppressions .."):
        for suppression in suppressions:
            # id is added by mongoengine
            suppression_id = str(suppression.id)  # ty: ignore[unresolved-attribute]
            col_delete, col_info = st.columns([0.15, 0.85])
            col_info.write(
                f"`{suppression.alert}` / `{suppression.instrument_id}` / "
                f"`{suppression.identifier}` / `{suppression.details}`: {suppression.comment}"
            )
            if col_delete.button(
                "Delete",
                key=f"delete_suppression_{suppression_id}",
                disabled=DISABLE_WRITE,
                icon=":material/delete:",
            ):
                try:
                    delete_alert_suppression(suppression_id)
                except Exception as e:  # noqa: BLE001
                    show_error_toast(str(e))
                else:
                    show_success_toast("Deleted alert suppression.")

st.markdown("### Add suppression")

with st.form("add_suppression_form"):
    c1, c2 = st.columns(2)
    alert = c1.text_input(
        "Alert*",
        value=ALERT_SUPPRESSION_WILDCARD,
        help="Class name of the alert, e.g. `PumpPressureHighAlert`.",
    )
    instrument_id = c2.selectbox(
        "Instrument*", options=[ALERT_SUPPRESSION_WILDCARD, *_INSTRUMENT_IDS]
    )
    identifier = c1.text_input(
        "Identifier*",
        value=ALERT_SUPPRESSION_WILDCARD,
        help="Exact identifier shown in the alert.",
    )
    details = c2.text_input(
        "Details*",
        value=ALERT_SUPPRESSION_WILDCARD,
        help="Part of the alert text.",
    )
    comment = st.text_area("Comment*", help="Why are these alerts muted?")
    c3, c4 = st.columns(2)
    expires_on = c3.date_input(
        "Expires after",
        value=None,
        min_value=display_now().date(),
        help="Last day the suppression applies. Leave empty to never expire.",
    )
    created_by = c4.text_input("Created by")

    st.write(r"\* Required fields")
    form_submit = st.form_submit_button(
        "Add suppression",
        disabled=DISABLE_WRITE,
        help="Temporarily disabled." if DISABLE_WRITE else "",
    )

if form_submit:
    try:
        validate_suppression(
            alert=alert,
            instrument_id=instrument_id,
            identifier=identifier,
            details=details,
            comment=comment,
        )
        add_alert_suppression(
            alert=alert.strip(),
            instrument_id=instrument_id,
            identifier=identifier.strip(),
            details=details.strip(),
            comment=comment.strip(),
            expires_at=end_of_day(expires_on) if expires_on else None,
            created_by=empty_to_none(created_by),
        )
    except Exception as e:  # noqa: BLE001
        show_error_toast(str(e))
    else:
        show_success_toast("Added alert suppression.")
