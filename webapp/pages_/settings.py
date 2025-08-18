"""Simple data overview."""

# ruff: noqa: TRY301 # Abstract `raise` to an inner function
import pandas as pd
import streamlit as st
from service.components import show_filter, show_sandbox_message
from service.db import (
    df_from_db_data,
    get_project_data,
    get_settings_data,
)
from service.query_params import get_all_query_params
from service.session_state import SessionStateKeys, set_session_state
from service.utils import (
    DISABLE_WRITE,
    _log,
    empty_to_none,
    quanting_settings_path,
    show_feedback_in_sidebar,
)

from shared.db.interface import add_settings
from shared.db.models import ProjectStatus
from shared.keys import SoftwareTypes
from shared.validation import validate_name
from shared.yamlsettings import YamlKeys, get_path

_log(f"loading {__file__} {get_all_query_params()}")
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


# ########################################### DISPLAY

st.markdown("## Current settings")


st.warning("This page should be edited only by administrators!", icon="⚠️")


@st.fragment
def display_settings(
    settings_df: pd.DataFrame, st_display: st.delta_generator.DeltaGenerator = st
) -> None:
    """Fragment to display settings in a table."""
    filtered_df, *_ = show_filter(settings_df, st_display=st_display)

    # beautify
    filtered_df = filtered_df.drop(columns=["_id"], errors="ignore")
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
        f"`{quanting_settings_path}/<project id>/`"
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

        If you want to update settings (e.g. use a newer AlphaDIA version or a different config file), just create a new settings entry
        for the corresponding project. The current settings will be set to 'inactive', and the version number will be increased.
        This version number is shown with each results metrics entry to keep track of the settings used for the analysis.

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


if selected_project:
    c1.markdown("### Step 2/3: Define settings")

    desc = f"('{selected_project.description}')" if selected_project.description else ""
    c1.write(
        f"Settings will be added to the following project: `{selected_project.name}`{desc}"
    )

    software_type = c1.selectbox(
        label="Type", options=[SoftwareTypes.ALPHADIA, SoftwareTypes.CUSTOM]
    )

    form_items = {
        "name": {
            "label": "Name*",
            "max_chars": 64,
            "placeholder": "e.g. 'very fast plasma settings'",
            "help": "Human readable short name for your settings.",
        },
        "fasta_file_name": {
            "label": "Fasta file name**",
            "max_chars": 64,
            "placeholder": "e.g. 'human.fasta'",
            "help": "Name of the fasta file.",
        },
        "speclib_file_name": {
            "label": "Speclib file name**",
            "max_chars": 64,
            "placeholder": "e.g. 'human_plasma.speclib'",
            "help": "Name of the speclib file.",
        },
    }

    if software_type == SoftwareTypes.ALPHADIA:
        form_items |= {
            "config_file_name": {
                "label": "Config file name*",
                "max_chars": 64,
                "placeholder": "e.g. 'very_fast_config.yaml'",
                "help": "Name of the config file. If none is given, default will be used.",
            },
            "software": {
                "label": "Software*",
                "max_chars": 64,
                "placeholder": "e.g. 'alphadia-1.10.0'",
                "help": "Name of the Conda environment that holds the AlphaDIA executable. Ask an administrator to created this environment..",
            },
        }

    elif software_type == SoftwareTypes.CUSTOM:
        form_items |= {
            "software": {
                "label": "Executable*",
                "max_chars": 64,
                "placeholder": "e.g. 'custom-software/custom-executable1.2.3'",
                "help": f"Path to executable, relative to `{get_path(YamlKeys.Locations.SOFTWARE)}/`. Ask an administrator to add the executable to the software folder.",
            },
            "config_params": {
                "label": "Configuration parameters",
                "max_chars": 512,
                "placeholder": "e.g. '--qvalue 0.01 --f RAW_FILE_PATH --out OUTPUT_PATH --temp OUTPUT_PATH --lib LIBRARY_PATH --fasta FASTA_PATH'",
                "help": "Configuration options. Provide either this OR config file name above, not both.",
            },
        }

    with c1.form("add_settings_to_project"):
        name = st.text_input(**form_items["name"])

        software = st.text_input(**form_items["software"])

        fasta_file_name = st.text_input(**form_items["fasta_file_name"])
        speclib_file_name = st.text_input(**form_items["speclib_file_name"])

        config_file_name = (
            st.text_input(**form_items["config_file_name"])
            if "config_file_name" in form_items
            else None
        )

        if "config_params" in form_items:
            config_params = st.text_area(**form_items["config_params"])
            st.info(
                "The following placeholders can be used in the config parameters:\n\n"
                "- `RAW_FILE_PATH`: Will evaluate to the path of the raw file.\n"
                "- `OUTPUT_PATH`: Will evaluate to the path of the output directory.\n"
                "- `LIBRARY_PATH`: Will evaluate to the path of the library file.\n"
                "- `FASTA_PATH`: Will evaluate to the path of the fasta file.\n"
            )
        else:
            config_params = None

        # Validate inputs and show errors
        validation_errors = []
        for to_validate in [
            fasta_file_name,
            speclib_file_name,
            software,
            config_file_name,
            config_params,
        ]:
            if to_validate:
                validation_errors.extend(validate_name(to_validate, allow_spaces=True))

        st.write(r"\* Required fields")
        st.write(r"\** At least one of the two must be given")

        st.markdown("### Step 3/3: Upload files to pool folder")
        st.markdown(
            "Make sure you have uploaded all the files correctly to "
            f"`{quanting_settings_path}/{project_id}/`"
        )
        # TODO: NEXT_SLICE add list of files to upload here
        upload_checkbox = st.checkbox(
            "I have uploaded the above files to this folder.", value=False
        )

        if "project" in settings_df.columns and len(
            settings_df[settings_df["project"] == selected_project.id]
        ):
            st.info(
                "When adding new settings, the current settings for this project will be set to 'inactive'."
            )

        submit = st.form_submit_button(
            f"Add settings to project {selected_project.id}",
            disabled=DISABLE_WRITE,
            help="Temporarily disabled." if DISABLE_WRITE else "",
        )


if selected_project and submit:
    try:
        if (
            empty_to_none(fasta_file_name) is None
            and empty_to_none(speclib_file_name) is None
        ):
            raise ValueError(
                "At least one of the fasta and speclib file names must be given."
            )  # Abstract `raise` to an inner function

        if validation_errors:
            errors_str = "\n- ".join(validation_errors)
            raise ValueError(f"Input validation error:\n- {errors_str}")

        if not upload_checkbox:
            raise ValueError(
                "Please upload the files to the respective folders on the pool file system and check the respective box."
            )

        add_settings(
            project_id=selected_project.id,
            name=empty_to_none(name),
            fasta_file_name=fasta_file_name,
            speclib_file_name=speclib_file_name,
            config_file_name=config_file_name,
            config_params=config_params,
            software_type=software_type,
            software=software,
        )
    except Exception as e:  # noqa: BLE001
        st.error(f"Error: {e}")
        set_session_state(SessionStateKeys.ERROR_MSG, f"{e}")
    else:
        set_session_state(
            SessionStateKeys.SUCCESS_MSG,
            f"Added new settings '{name}' to project {selected_project.id}.",
        )
    st.rerun()
