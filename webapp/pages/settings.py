"""Simple data overview."""

import os

# ruff: noqa: TRY301 # Abstract `raise` to an inner function
import pandas as pd
import streamlit as st
from service.components import show_filter, show_sandbox_message
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

from shared.db.interface import add_new_settings_to_db
from shared.db.models import ProjectStatus
from shared.keys import EnvVars

_log(f"loading {__file__}")
# ########################################### PAGE HEADER

st.set_page_config(page_title="AlphaKraken: settings", layout="wide")

show_sandbox_message()

st.markdown("# Settings")

# ########################################### SIDEBAR

show_feedback_in_sidebar()


# ########################################### LOGIC

settings_db = get_settings_data()
projects_db = get_project_data()
settings_df = df_from_db_data(settings_db)

quanting_pool_folder = os.environ.get(EnvVars.QUANTING_POOL_FOLDER)

# ########################################### DISPLAY

st.markdown("## Current settings")


st.warning("This page should be edited only by AlphaKraken admin users!", icon="⚠️")


@st.experimental_fragment
def display_settings(
    settings_df: pd.DataFrame, st_display: st.delta_generator.DeltaGenerator = st
) -> None:
    """Fragment to display settings in a table."""
    filtered_df = show_filter(settings_df, st_display=st_display)

    # beautify
    del filtered_df["_id"]
    st_display.table(
        filtered_df.style.apply(
            lambda row: [
                "color: lightgray"
                if row["status"] == ProjectStatus.INACTIVE
                else "background-color: white"
            ]
            * len(row),
            axis=1,
        )
    )

    st_display.markdown(
        "The files associated with the settings of a given project are stored at "
        f"`/fs/pool/{quanting_pool_folder}/settings/<project id>/`"
    )


display_settings(settings_df)

c1, _ = st.columns([0.5, 0.5])
with c1.expander("Click here for help ..."):
    st.info(
        """
        ### Explanation
        Settings are a defined tuple of input to the quanting software: config file, speclib file and/or fasta file
        which will be used to process all files associated to the parent project.
        Before creating the settings on this page, make sure you copied the corresponding files to the project-specific pool folder.

        Note: currently, updates of settings are not possible. If you need to change them, ask an AlphaKraken admin.

        ### Workflow
        1. Select a project to associate the new settings with.
        2. Fill in required information (file names, etc.).
        3. Upload the files to the designated location.
        """,
        icon="ℹ️",  # noqa: RUF001
    )

# ########################################### SELECT PROJECT

c1.markdown("## Add new settings for project")
c1.markdown("### Step 1/3: Select project")

project_info = [""] + [p.id for p in projects_db]

project_id = c1.selectbox(
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
    # TODO: make software selection options dynamic
    "software": {
        "label": "software",
        "options": [
            "alphadia-1.8.2",
            "alphadia-1.7.2",
        ],
    },
}


if selected_project:
    c1.markdown("### Step 2/3: Define settings")

    desc = f"('{selected_project.description}')" if selected_project.description else ""
    c1.write(
        f"Settings will be added to the following project: `{selected_project.name}`{desc}"
    )

    with c1.form("add_settings_to_project"):
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
            f"`/fs/pool/{quanting_pool_folder}/settings/{project_id}/`"
        )
        upload_checkbox = st.checkbox(
            "I have uploaded the above files to this folder.", value=False
        )

        if "project" in settings_df.columns and len(
            settings_df[settings_df["project"] == selected_project.id]
        ):
            st.info("The current settings for this project will be set to 'inactive'.")

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
