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
    assign_settings_to_project,
    get_all_settings,
)

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
        f"Output files are stored at `{quanting_output_path}/<project id>/out_<raw file name>/`. In case you don't know your project ID, it's most likely `_FALLBACK`."
    )


display_projects(projects_df)

# ########################################### ASSIGN SETTINGS

st.markdown("## Assign settings to project")

c_assign1, c_assign2 = st.columns([0.5, 0.5])

with c_assign1:
    st.markdown("### Select project and settings")

    project_options = [""] + [p.id for p in projects_db]
    selected_project_id = st.selectbox(
        "Select project", options=project_options, key="assign_project_select"
    )

    if selected_project_id:
        selected_project = projects_db(id=selected_project_id).first()

        if selected_project.settings:
            current_settings_text = (
                f"{selected_project.settings.name} v{selected_project.settings.version}"
            )
            st.info(f"Current settings: **{current_settings_text}**")
        else:
            st.info("No settings currently assigned to this project.")

        all_settings = get_all_settings(include_archived=False)

        REMOVE_ASSIGNMENT_ = "(Remove assignment)"

        if not all_settings:
            st.warning("No active settings available. Create settings first.")
        else:
            settings_options_map = {REMOVE_ASSIGNMENT_: None} | {
                f"{s.name} v{s.version}": str(s.id)  # type: ignore[unresolved-attribute]
                for s in all_settings
            }

            selected_settings_display = st.selectbox(
                "Select settings to assign (or remove)",
                options=settings_options_map.keys(),
                key="assign_settings_select",
            )

            if st.button(
                "Update settings assignment",
                disabled=DISABLE_WRITE,
                help="Temporarily disabled." if DISABLE_WRITE else "",
            ):
                try:
                    new_settings_id = settings_options_map[selected_settings_display]
                    assign_settings_to_project(selected_project_id, new_settings_id)
                    set_session_state(
                        SessionStateKeys.SUCCESS_MSG,
                        f"Assigned settings '{selected_settings_display}' to project {selected_project_id}.",
                    )
                    st.rerun()
                except Exception as e:  # noqa: BLE001
                    st.error(f"Error: {e}")
                    set_session_state(SessionStateKeys.ERROR_MSG, f"{e}")

with c_assign2:
    st.markdown("### Help")
    st.info(
        """
        Use this section to assign settings to projects or change the settings assignment.

        - Projects can have zero or one settings assigned
        - Multiple projects can share the same settings
        - You can remove a settings assignment by selecting "(Remove assignment)"
        - Only active (non-archived) settings can be assigned
        """,
        icon="ℹ️",  # noqa: RUF001
    )

# display_projects(projects_df)

# ########################################### FORM

form_items = {
    "project_name": {
        "label": "Project Name*",
        "max_chars": 64,
        "placeholder": "e.g. Plasma project 42",
        "help": "Human-readable name of the project.",
    },
    "project_id": {
        "label": "Project Id*",
        "max_chars": 16,
        "placeholder": "e.g. P1234",
        "help": "Unique identifier of the project. This needs to be put in every file name in order to have it associated with this project. "
        "Exception: the special project id '_FALLBACK' will be used for files that do not belong to any project.",
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
        ### Explanation
        A 'project' is a lightweight container that can reference quanting settings (=config, speclib, fasta).
        The connection of samples to a project is done via the file name: all files containing a project-specific token (e.g. `A123`) surrounded by `_`
        are associated with project `A123`.
        Currently, only projects ids that follow after the pattern 'SA' are picked up, e.g. `20240801_something_SA_A123_my-sample.raw`.
        If no matching project can be found for a file, then fallback settings are used.
        Please make sure your project identifier is 'unique enough' ("DDA" might be a bad pick), otherwise it might cause false positives.
        Needs to be between 3 and 8 characters, contain only uppercase letters and numbers, and at least one letter.

        ### Workflow
        1. Create a project with a unique project id.
        2. Create settings on the 'Settings' page if needed.
        3. Use the 'Assign settings to project' section above to link settings to your project.
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
SPECIAL_PROJECT_IDS = ["_FALLBACK", "_FALLBACK_BRUKER"]


def _check_project_id(project_id: str) -> None:
    """Check if the project id is valid, raise ValueError if not."""
    if (
        project_id is None
        or len(project_id) < 3  # noqa: PLR2004
        or len(project_id) > 8  # noqa: PLR2004
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
