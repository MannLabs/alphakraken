"""Page allowing Project mgmt."""

import pandas as pd
import streamlit as st
from db.interface import add_new_project_to_db
from service.components import show_filter
from service.db import df_from_db_data, get_project_data
from service.utils import (
    SessionStateKeys,
    _log,
    empty_to_none,
    show_feedback_in_sidebar,
)

_log(f"loading {__file__}")

# ########################################### PAGE HEADER

st.set_page_config(page_title="AlphaKraken: projects", layout="wide")
st.markdown("# Projects")

st.markdown("## Current projects")


# ########################################### SIDEBAR

show_feedback_in_sidebar()


# ########################################### LOGIC

projects_db = get_project_data()
projects_df = df_from_db_data(projects_db)


# ########################################### DISPLAY

st.warning("This page should currently be edited only by admin users!", icon="⚠️")


@st.experimental_fragment
def display_projects(projects_df: pd.DataFrame) -> None:
    """A Fragment to display projects in a table."""
    filtered_df = show_filter(projects_df)
    st.table(filtered_df)


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


st.markdown("## Add new project")

with st.form("create_project_form"):
    project_name = st.text_input(**form_items["project_name"])
    project_id = st.text_input(**form_items["project_id"])
    project_description = st.text_area(**form_items["project_description"])

    st.write(r"\* Required fields")
    form_submit = st.form_submit_button("Create project")


if form_submit:
    try:
        add_new_project_to_db(
            project_id=empty_to_none(project_id),
            name=empty_to_none(project_name),
            description=project_description,
        )
    except Exception as e:  # noqa: BLE001
        st.error(f"Error: {e}")
        st.session_state[SessionStateKeys.ERROR_MSG] = f"{e}"
    else:
        st.session_state[SessionStateKeys.SUCCESS_MSG] = (
            f"Added new project '{project_id}' to the database."
        )
    st.rerun()
