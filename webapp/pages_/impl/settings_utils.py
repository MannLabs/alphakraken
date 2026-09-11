"""Form definitions and help texts for the settings page."""

from typing import Any

import pandas as pd
import streamlit as st
import streamlit.delta_generator

from shared.config_params import PLACEHOLDER_DESCRIPTIONS, PLACEHOLDER_LOCATIONS
from shared.display_paths import DISPLAY_PATHS, get_display_settings_path
from shared.keys import JobEngines, SoftwareTypes
from shared.path_views import Locations
from shared.runners import get_runner

# stand-in shown where the settings name is not known yet
SETTINGS_NAME_PLACEHOLDER = "<settings name>"


def get_settings_using_software(
    all_settings_df: pd.DataFrame, software_type: str, runner_name: str
) -> pd.DataFrame:
    """Get the settings that run `software_type` on `runner_name`, most recently created first."""
    required_columns = ["software", "software_type", "runner_name", "name", "version"]
    if all_settings_df.empty or not set(required_columns) <= set(
        all_settings_df.columns
    ):
        return pd.DataFrame(columns=required_columns)

    return all_settings_df[
        (all_settings_df["software_type"] == software_type)
        & (all_settings_df["runner_name"] == runner_name)
    ]


def build_form_items(software_type: str) -> dict[str, dict[str, Any]]:
    """Get the label, placeholder and help text of each input field of `software_type`."""
    form_items = {
        "name": {
            "label": "Settings Name*",
            "max_chars": 64,
            "placeholder": "e.g. 'plasma_fast' or 'hela_qc'",
            "help": "Alphanumeric + underscore only. Used as folder name and for versioning.",
        },
        "description": {
            "label": "Description",
            "max_chars": 512,
            "placeholder": "(optional) e.g. 'Fast plasma settings for routine analysis'",
            "help": "Human readable description of these settings.",
        },
    }

    if software_type == SoftwareTypes.ALPHADIA:
        form_items |= {
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
            "config_file_name": {
                "label": "Config file name*",
                "max_chars": 64,
                "placeholder": "e.g. 'very_fast_config.yaml'",
                "help": "Name of the config file. If none is given, AlphaDIA's default config will be used.",
            },
            "software": {
                "label": "Software*",
                "max_chars": 64,
                "placeholder": "e.g. 'alphadia-2.0.0'",
                "help": "Name of the conda environment that holds the AlphaDIA executable. Ask an administrator to create this environment.",
            },
        }

    elif software_type == SoftwareTypes.MSQC:
        form_items |= {
            "software": {
                "label": "Software*",
                "max_chars": 64,
                "placeholder": "e.g. 'msqc/run_msqc.sh'",
                "help": f"Path to executable, relative to `{DISPLAY_PATHS[Locations.SOFTWARE]}/`. Ask an administrator to add the executable to the software folder.",
            },
        }

    elif software_type == SoftwareTypes.SKYLINE:
        form_items |= {
            "software": {
                "label": "Software*",
                "max_chars": 64,
                "placeholder": "e.g. 'skyline/run_skyline.sh'",
                "help": f"Path to executable, relative to `{DISPLAY_PATHS[Locations.SOFTWARE]}/`. Ask an administrator to add the executable to the software folder.",
            },
            "config_params": {
                "label": "Configuration parameters",
                "max_chars": 2048,
                "placeholder": "e.g. '--in iRT_windows.sky --irt-database-path irt_c18_official.irtdb --report-add custom_iRT_report.skyr'",
                "help": "Configuration options for the Skyline software. Certain placeholders will be substituted.",
            },
        }
    else:
        form_items |= {
            "software": {
                "label": "Executable*",
                "max_chars": 64,
                "placeholder": "e.g. 'custom-software/custom-executable1.2.3'",
                "help": f"Path to executable, relative to `{DISPLAY_PATHS[Locations.SOFTWARE]}/`. Ask an administrator to add the executable to the software folder. "
                f"For a runner with the `{JobEngines.DOCKER}` engine, this is a docker image name instead, e.g. `alphakraken-msqc`. "
                f"The image must already be present on the worker host, ask an administrator to add it. ",
            },
            "config_params": {
                "label": "Configuration parameters",
                "max_chars": 2048,
                "placeholder": "e.g. '--qvalue 0.01 --f {{RAW_FILE_PATH}} --lib {{SETTINGS_PATH}}/library.speclib --fasta {{SETTINGS_PATH}}/human.fasta --temp {{OUTPUT_PATH}} --threads {{NUM_THREADS}}'",
                "help": "Configuration options for the custom software. Certain placeholders will be substituted.",
            },
        }

    return form_items


def show_help(st_display: streamlit.delta_generator.DeltaGenerator) -> None:
    """Show the expander explaining what settings are and how to create them."""
    with st_display.expander("Click here for help ..."):
        st.info(
            f"""
        ### Explanation
        Settings are a defined tuple that decides what software is run and its parameters.
        Settings are standalone entities that can be shared across multiple projects.

        ### Workflow
        1. Choose "Create new settings..." to define a brand new settings configuration or select an existing settings name to update it to a new version.
        2. Fill in required information (software, file names, etc.).
        3. Make sure the software is available and upload the files to the designated location: `{get_display_settings_path(SETTINGS_NAME_PLACEHOLDER)}/`
        4. Submit
        4. Assign the settings to projects on the "Manage projects" page.

        ### Versioning
        Settings use name + version as a unique identifier. If you create settings with an existing name, the version number will automatically increment.
        This allows you to update settings (e.g., use a newer AlphaDIA version) while keeping old versions available.

        **Important:** Projects always reference a specific version of settings (e.g., 'plasma_fast' v2).
        Creating a new version does not affect existing projects - they continue using their assigned version until the assignment is explicitly updated on the "Manage projects" page.
        """,
            icon="ℹ️",  # noqa: RUF001
        )


def show_placeholder_info(
    st_display: streamlit.delta_generator.DeltaGenerator, runner_name: str
) -> None:
    """Show the placeholders usable in the config parameters, resolved as `runner_name` sees them."""
    runner_view = get_runner(runner_name).view
    placeholder_list = "\n".join(
        f"- `{{{{{placeholder}}}}}`: {description}"
        + (
            f", below `{runner_view.resolve(PLACEHOLDER_LOCATIONS[placeholder])}`"
            if placeholder in PLACEHOLDER_LOCATIONS
            else ""
        )
        for placeholder, description in PLACEHOLDER_DESCRIPTIONS.items()
    )
    st_display.info(
        "The following placeholders can be used in the config parameters, and will be replaced by the specified values "
        f"(paths as runner `{runner_name}` sees them):\n\n"
        f"{placeholder_list}\n\n"
        f"Notes:\n"
        "- Your uploaded input files are available under `{{SETTINGS_PATH}}}`, e.g. `{{SETTINGS_PATH}}/human.fasta`.\n"
        "- The working directory of the software is `{{OUTPUT_PATH}}`.\n"
        "- If something that is in the `$PATH` should be executed (e.g. `apptainer`), wrap it in a shell script and ask an admin to place it in the software folder.\n"
    )


def show_software_examples(
    st_display: streamlit.delta_generator.DeltaGenerator,
) -> None:
    """Show the expanders with example executables and config parameters."""
    with st_display.expander("Example for DIANN..."):
        st.write("Executable: `diann/diann-linux`")
        st.code(
            "--f {{RAW_FILE_PATH}} --lib {{SETTINGS_PATH}}/library.speclib --fasta {{SETTINGS_PATH}}/human.fasta --temp {{OUTPUT_PATH}} --threads {{NUM_THREADS}} --qvalue 0.01"
        )
    with st_display.expander("Example for Spectronaut..."):
        st.write("Executable: `spectronaut/run_spectronaut.sh`")
        st.code(
            "direct -n alphakraken -r {{RAW_FILE_PATH}} -fasta {{SETTINGS_PATH}}/human.fasta -o {{OUTPUT_PATH}} -s {{SETTINGS_PATH}}/alphakraken.prop"
        )
