"""Page allowing Project mgmt."""

import re
from typing import Any

import pandas as pd
import streamlit as st
import streamlit.delta_generator
from service.components import show_filter, show_sandbox_message
from service.db import (
    df_from_db_data,
    get_project_data,
)
from service.query_params import get_all_query_params
from service.session_state import SessionStateKeys, set_session_state
from service.utils import (
    DISABLE_WRITE,
    _log,
    empty_to_none,
    quanting_output_path,
    show_feedback_in_sidebar,
)

from shared.db.interface import (
    add_project,
    create_project_settings,
    get_all_settings,
    get_project_settings,
    remove_project_settings,
)
from shared.keys import DEFAULT_SCOPE, KNOWN_VENDOR_NAMES
from shared.scope import resolve_scoped_settings
from shared.yamlsettings import YAMLSETTINGS, YamlKeys

_INSTRUMENTS_CONFIG = YAMLSETTINGS.get(YamlKeys.INSTRUMENTS, {})
_INSTRUMENT_IDS = list(_INSTRUMENTS_CONFIG.keys())
SCOPE_OPTIONS = [DEFAULT_SCOPE, *KNOWN_VENDOR_NAMES, *_INSTRUMENT_IDS]

_log(f"loading {__file__} {get_all_query_params()}")

# ########################################### PAGE HEADER

st.set_page_config(page_title="AlphaKraken: projects", layout="wide")

show_sandbox_message()

st.markdown("# Projects")

st.markdown("## Current projects")


# ########################################### SIDEBAR

show_feedback_in_sidebar()


# ########################################### LOGIC

projects_db = get_project_data()
projects_df = df_from_db_data(projects_db)

# build settings names column via ProjectSettings M:N relationship
_project_to_settings: dict[str, list[str]] = {}
for p in projects_db:
    ps_list = get_project_settings(p.id)
    if ps_list:
        _project_to_settings[p.id] = [
            f"{ps.settings.name} version {ps.settings.version}" for ps in ps_list
        ]

if not projects_df.empty:
    projects_df["settings"] = projects_df["_id"].map(
        lambda pid: ", ".join(_project_to_settings.get(pid, []))
    )


# ########################################### DISPLAY

st.warning("This page should be edited only by administrators!", icon="⚠️")


@st.fragment
def display_projects(
    projects_df: pd.DataFrame,
    st_display: st.delta_generator.DeltaGenerator | Any = None,
) -> None:
    """A Fragment to display projects in a table."""
    if st_display is None:
        st_display = st
    filtered_df, *_ = show_filter(projects_df, st_display=st_display)

    st_display.table(filtered_df)
    st_display.markdown(
        f"Output files are stored at `{quanting_output_path}/<project id>/out_<raw file name>/<settings_type>/`. In case you don't know your project ID, it's most likely `_FALLBACK`."
    )


display_projects(projects_df)


c1, _ = st.columns([0.5, 0.5])
with c1.expander("Click here for help ..."):
    st.info(
        """
        ### Explanation
        A 'project' is a lightweight container that can reference quanting settings (=config, speclib, fasta).
        The connection of samples to a project is done via the file name: all files containing a project-specific token (e.g. `A123`) surrounded by `_`
        are associated with project `A1234`.
        Currently, only projects ids that follow after the pattern 'SA' are picked up, e.g. `20240801_something_SA_A123_my-sample.raw`.
        If no matching project can be found for a file, then fallback settings are used.
        Please make sure your project identifier is 'unique enough' ("DDA" might be a bad pick), otherwise it might cause false positives.
        Needs to be between 5 and 16 characters, contain only uppercase letters and numbers, and at least one letter.

        ### Workflow
        1. Create a project with a unique project id.
        2. Create settings on the 'Settings' page if needed.
        3. Use the 'Manage project settings' section above to link settings to your project.
        """,
        icon="ℹ️",  # noqa: RUF001
    )


# ########################################### ASSIGN SETTINGS

st.markdown("## Manage project settings")

c_assign1, c_assign2 = st.columns([0.5, 0.5])

with c_assign1:
    project_options = [""] + [p.id for p in projects_db]
    selected_project_id = st.selectbox(
        "Select project", options=project_options, key="assign_project_select"
    )

    if selected_project_id:
        current_ps_list = get_project_settings(selected_project_id)

        if current_ps_list:
            st.markdown("**Current settings assignments:**")
            for ps in current_ps_list:
                col_info, col_btn = st.columns([0.9, 0.1])
                excluded_str = (
                    f" [excluded: {', '.join(ps.excluded)}]" if ps.excluded else ""
                )
                col_info.write(
                    f"`[scope: {ps.scope}]` '{ps.settings.name}' version {ps.settings.version} (type: `{ps.settings.software_type}`, executable: `{ps.settings.software}`){excluded_str}"
                )
                ps_id = str(ps.id)  # type: ignore[unresolved-attribute]
                if col_btn.button(
                    "",
                    key=f"remove_ps_{ps_id}",
                    disabled=DISABLE_WRITE,
                    help=f"Unassign '{ps.settings.name}' from this project. {'Temporarily disabled.' if DISABLE_WRITE else ''}",
                    icon=":material/link_off:",
                ):
                    try:
                        remove_project_settings(ps_id)  # type: ignore[unresolved-attribute]
                        set_session_state(
                            SessionStateKeys.SUCCESS_MSG,
                            f"Removed settings assignment from project {selected_project_id}.",
                        )
                        st.rerun()
                    except Exception as e:  # noqa: BLE001
                        st.error(f"Error: {e}")
                        set_session_state(SessionStateKeys.ERROR_MSG, f"{e}")
        else:
            st.info("No settings assigned to this project.")

        all_settings = get_all_settings(include_archived=False)

        if not all_settings:
            st.warning("No active settings available. Create settings first.")
        else:
            settings_options_map = {
                f"{s.name} version {s.version} (type: {s.software_type}, executable: {s.software})": str(
                    s.id  # type: ignore[unresolved-attribute]
                )
                for s in all_settings
            }

            c1, c2, c3 = st.columns([0.5, 0.25, 0.25])
            selected_settings_display = c1.selectbox(
                "Select settings to assign",
                options=settings_options_map.keys(),
                key="assign_settings_select",
            )

            selected_scope = c2.selectbox(
                "Select instrument or vendor scope",
                options=SCOPE_OPTIONS,
                key="assign_scope_select",
                help="'*' = all instruments, vendor name = vendor-specific, instrument ID = instrument-specific",
            )

            selected_excluded = c3.multiselect(
                "Exclude instruments from scope (optional)",
                options=_INSTRUMENT_IDS,
                key="assign_excluded_select",
                help="Instruments to exclude from this scope assignment.",
            )

            if st.button(
                f"Assign selected settings to project {selected_project_id}",
                disabled=DISABLE_WRITE,
                help=f"Assign of the selected settings to the project with the specified scope. {'Temporarily disabled.' if DISABLE_WRITE else ''}",
                icon=":material/link:",
            ):
                try:
                    new_settings_id = settings_options_map[selected_settings_display]
                    create_project_settings(
                        selected_project_id,
                        new_settings_id,
                        scope=selected_scope,
                        excluded=selected_excluded,
                    )
                    set_session_state(
                        SessionStateKeys.SUCCESS_MSG,
                        f"Assigned settings '{selected_settings_display}' to project {selected_project_id}.",
                    )
                    st.rerun()
                except Exception as e:  # noqa: BLE001
                    st.error(f"Error: {e}")
                    set_session_state(SessionStateKeys.ERROR_MSG, f"{e}")

with c_assign2:
    if selected_project_id:
        st.markdown(
            "### Resolved settings per instrument",
            help="This table shows the settings that are applied for each instrument based on the current settings assignments and their scopes.",
        )
        current_ps_for_table = get_project_settings(selected_project_id)
        rows = []
        for instr_id in _INSTRUMENT_IDS:
            instr_type = _INSTRUMENTS_CONFIG.get(instr_id, {}).get(YamlKeys.TYPE, "")
            resolved = resolve_scoped_settings(
                current_ps_for_table, instr_id, instr_type
            )
            if resolved:
                for s in resolved:
                    detail_parts = [
                        f"description: {s.description}" if s.description else None,
                        f"config_file: {s.config_file_name}"
                        if s.config_file_name
                        else None,
                        f"fasta: {s.fasta_file_name}" if s.fasta_file_name else None,
                        f"speclib: {s.speclib_file_name}"
                        if s.speclib_file_name
                        else None,
                        f"config_params: {s.config_params}"
                        if s.config_params
                        else None,
                    ]
                    rows.append(
                        {
                            "instrument": instr_id,
                            "settings": f"{s.name} version {s.version} ({s.software_type})",
                            "software": s.software,
                            "details": " | ".join(p for p in detail_parts if p),
                        }
                    )
            else:
                rows.append(
                    {
                        "instrument": instr_id,
                        "settings": "--",
                        "software": "--",
                        "details": "",
                    }
                )
        resolved_df = pd.DataFrame(rows)
        instrument_color_map = {
            instr: idx % 2
            for idx, instr in enumerate(resolved_df["instrument"].unique())
        }
        RESOLVED_TABLE_COLORS = [
            "#f0f0f0",
            "white",
        ]
        st.table(
            resolved_df.style.apply(
                lambda row: [
                    f"background-color: {RESOLVED_TABLE_COLORS[instrument_color_map[row['instrument']]]}"
                ]
                * len(row),
                axis=1,
            )
        )
        st.page_link(
            "pages_/settings.py",
            label="➔ Go to settings page to create/edit settings",
            icon="📋",
        )
    else:
        st.info(
            "Select a project on the left to see resolved settings per instrument.",
            icon="ℹ️",  # noqa: RUF001
        )

# display_projects(projects_df)

# ########################################### FORM

form_items = {
    "project_id": {
        "label": "Project Id*",
        "max_chars": 16,
        "placeholder": "e.g. P1234",
        "help": "Unique identifier of the project. This needs to be put in every file name in order to have it associated with this project. "
        "Exception: the special project id '_FALLBACK' will be used for files that do not belong to any project.",
    },
    "project_name": {
        "label": "Project Name*",
        "max_chars": 64,
        "placeholder": "e.g. Plasma project 42",
        "help": "Human-readable name of the project.",
    },
    "project_description": {
        "label": "Project Description",
        "max_chars": 256,
        "placeholder": "(optional)",
        "help": "Human-readable project details.",
    },
}

c1, _ = st.columns([0.5, 0.5])
with c1.expander("Click here for help ..."):
    st.info(
        """
        Use this section to add/remove assignements of settings to projects.

        - Projects can have multiple settings assigned
        - Multiple projects can share the same settings
        - You can remove individual settings assignments using the "Remove" button
        - Only active (non-archived) settings can be assigned
        - A certain software type can only be assigned once for a scope
        - The "scope" defines for which instruments the settings should be applied. `*` means all instruments, otherwise you can choose a specific vendor or instrument id. If multiple settings match for a given instrument, then the most specific one gets picked (e.g. instrument-specific over vendor-specific over '*').
        """,
        icon="ℹ️",  # noqa: RUF001
    )


c1.markdown("## Add new project")

with c1.form("create_project_form"):
    project_name = st.text_input(**form_items["project_name"])
    project_id = st.text_input(**form_items["project_id"])
    project_description = st.text_area(**form_items["project_description"])

    st.write(r"\* Required fields")
    form_submit = st.form_submit_button(
        "Create project",
        disabled=DISABLE_WRITE,
        help="Temporarily disabled." if DISABLE_WRITE else "",
    )


ALLOWED_CHARACTERS_IN_PROJECT_ID = r"[^A-Z0-9]"
FORBIDDEN_PROJECT_IDS = ["dda", "dia"]
SPECIAL_PROJECT_IDS = ["_FALLBACK"]


def _check_project_id(project_id: str) -> None:
    """Check if the project id is valid, raise ValueError if not."""
    if (
        project_id is None
        or len(project_id) < 5  # noqa: PLR2004
        or len(project_id) > 16  # noqa: PLR2004
        or project_id.isdigit()
        or re.findall(ALLOWED_CHARACTERS_IN_PROJECT_ID, project_id)
        or project_id.lower() in FORBIDDEN_PROJECT_IDS
    ) and project_id not in SPECIAL_PROJECT_IDS:
        raise ValueError(
            f"Invalid project id '{project_id}'. Please choose a different one."
        )


if form_submit:
    try:
        _check_project_id(empty_to_none(project_id))

        add_project(
            project_id=empty_to_none(project_id),
            name=empty_to_none(project_name),
            description=project_description,
        )
    except Exception as e:  # noqa: BLE001
        st.error(f"Error: {e}")
        set_session_state(SessionStateKeys.ERROR_MSG, f"{e}")
    else:
        set_session_state(
            SessionStateKeys.SUCCESS_MSG,
            f"Added new project '{project_id}' to the database.",
        )
    st.rerun()
