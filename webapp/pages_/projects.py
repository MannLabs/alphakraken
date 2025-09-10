"""Page allowing Project mgmt."""

import re

import pandas as pd
import streamlit as st
from service.components import show_filter, show_sandbox_message
from service.db import df_from_db_data, get_project_data
from service.query_params import get_all_query_params
from service.session_state import SessionStateKeys, set_session_state
from service.utils import (
    DISABLE_WRITE,
    _log,
    empty_to_none,
    quanting_output_path,
    show_feedback_in_sidebar,
)

from shared.db.interface import add_project

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
    projects_df: pd.DataFrame, st_display: st.delta_generator.DeltaGenerator = st
) -> None:
    """A Fragment to display projects in a table."""
    filtered_df, *_ = show_filter(projects_df, st_display=st_display)
    st_display.table(filtered_df)
    st_display.markdown(
        f"Output files are stored at `{quanting_output_path}/<project id>/out_<raw file name>/`. In case you don't know your project ID, it's most likely `_FALLBACK`."
    )


display_projects(projects_df)

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
        A 'project' is a lightweight container for a tuple of quanting settings (=config, speclib, fasta).
        The connection of samples to a project is done via the file name: all files containing a project-specific token (e.g. `A123`) surrounded by `_`
        are associated with project `A123`.
        Currently, only projects ids that follow after the pattern 'SA' are picked up, e.g. `20240801_something_SA_A123_my-sample.raw`.
        If no matching project can be found for a file, then fallback settings are used.
        Please make sure your project identifier is 'unique enough' ("DDA" might be a bad pick), otherwise it might cause false positives.
        Needs to be between 3 and 8 characters, contain only uppercase letters and numbers, and at least one letter.

        ### Workflow
        1. Create a project with a unique project id.
        2. Use the 'settings' tab to associate quanting settings with the project.
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
