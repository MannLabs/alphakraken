"""Simple data overview."""

import os

# ruff: noqa: TRY301 # Abstract `raise` to an inner function
import pandas as pd
import streamlit as st
from db.interface import add_new_settings_to_db
from service.components import show_filter
from service.db import (
    df_from_db_data,
    get_project_data,
    get_settings_data,
)
from service.utils import (
    SessionStateKeys,
    _log,
    empty_to_none,
    show_feedback_in_sidebar,
)

from shared.keys import EnvVars

_log(f"loading {__file__}")
# ########################################### PAGE HEADER

st.set_page_config(page_title="AlphaKraken: settings", layout="wide")
st.markdown("# Settings")

# ########################################### SIDEBAR

show_feedback_in_sidebar()


# ########################################### LOGIC

settings_db = get_settings_data()
projects_db = get_project_data()
settings_df = df_from_db_data(settings_db)

io_pool_folder = os.environ.get(EnvVars.IO_POOL_FOLDER)

# ########################################### DISPLAY

st.markdown("## Current settings")


st.warning("This page should currently be edited only by admin users!", icon="⚠️")


@st.experimental_fragment
def display_settings(settings_df: pd.DataFrame) -> None:
    """Fragment to display settings in a table."""
    filtered_df = show_filter(settings_df)
    st.table(filtered_df)
    st.markdown(
        "The settings of a given project are stored at "
        f"`/fs/pool/{io_pool_folder}/settings/<project id>/`"
    )


display_settings(settings_df)

# ########################################### SELECT PROJECT

st.markdown("## Add new settings for project")
st.markdown("### Step 1/3: Select project")

project_info = [""] + [p.id for p in projects_db]

project_id = st.selectbox(
    label="Select project to add your settings to", options=project_info
)
project_id = empty_to_none(project_id)

selected_project = None
if project_id:
    selected_project = projects_db(id=project_id).first()

# ########################################### FORM


form_items = {
    "name": {
        "label": "Name*",
        "max_chars": 64,
        "placeholder": "e.g. very fast plasma settings.",
        "help": "Human readable short name for your settings.",
    },
    "fasta_file_name": {
        "label": "Fasta file name*",
        "max_chars": 64,
        "placeholder": "e.g. human.fasta",
        "help": "Name of the fasta file.",
    },
    "speclib_file_name": {
        "label": "Speclib file name*",
        "max_chars": 64,
        "placeholder": "e.g. human_plasma.speclib",
        "help": "Name of the speclib file.",
    },
    "config_file_name": {
        "label": "Config file name",
        "max_chars": 64,
        "placeholder": "e.g. very_fast_config.yaml",
        "help": "Name of the config file. If none is given, default will be used.",
    },
    # TODO: make software selection
    "software": {
        "label": "software",
        "options": [
            "alphadia-1.7.0",
            "alphadia-1.6.2",
        ],
    },
}


if selected_project:
    st.markdown("### Step 2/3: Define settings")

    desc = f"('{selected_project.description}')" if selected_project.description else ""
    st.write(
        f"Settings will be added to the following project: `{selected_project.name}`{desc}"
    )

    with st.form("add_settings_to_project"):
        name = st.text_input(**form_items["name"])
        fasta_file_name = st.text_input(**form_items["fasta_file_name"])
        speclib_file_name = st.text_input(**form_items["speclib_file_name"])
        config_file_name = st.text_input(**form_items["config_file_name"])
        software = st.selectbox(**form_items["software"])

        st.write(r"\* Required fields")
        st.write(r"\** At least one of the two must be given")

        st.markdown("### Step 3/3: Upload files to pool folder")
        st.markdown(
            "Make sure you have uploaded all the files correctly to "
            f"`/fs/pool/{io_pool_folder}/settings/{project_id}/`"
        )
        upload_checkbox = st.checkbox(
            "I have uploaded the above files to this folder.", value=False
        )

        submit = st.form_submit_button(f"Add settings to project {selected_project.id}")


if selected_project and submit:
    try:
        if (
            empty_to_none(fasta_file_name) is None
            and empty_to_none(speclib_file_name) is None
        ):
            raise ValueError(
                "At least one of the fasta and speclib file names must be given."
            )  # Abstract `raise` to an inner function

        if not upload_checkbox:
            raise ValueError(
                "Please upload the files to the respective folders on the pool file system and check the respective box."
            )

        add_new_settings_to_db(
            project_id=selected_project.id,
            name=empty_to_none(name),
            fasta_file_name=fasta_file_name,
            speclib_file_name=speclib_file_name,
            config_file_name=config_file_name,
            software=software,
        )
    except Exception as e:  # noqa: BLE001
        st.error(f"Error: {e}")
        st.session_state[SessionStateKeys.ERROR_MSG] = f"{e}"
    else:
        st.session_state[SessionStateKeys.SUCCESS_MSG] = (
            f"Added new settings '{name}' to project {selected_project.id}."
        )
    st.rerun()
