"""Settings management page."""

import re
from collections import defaultdict

# ruff: noqa: TRY301 # Abstract `raise` to an inner function
from typing import Any

import pandas as pd
import streamlit as st
import streamlit.delta_generator
from pages_.impl.settings_utils import (
    SETTINGS_NAME_PLACEHOLDER,
    build_form_items,
    get_settings_using_software,
    show_help,
    show_placeholder_info,
    show_software_examples,
)
from service.components import show_filter, show_sandbox_message
from service.db import (
    df_from_db_data,
    get_project_data,
    get_settings_data,
)
from service.query_params import get_all_query_params
from service.settings_validation import check_runner_supports_software_type
from service.utils import (
    DISABLE_WRITE,
    _log,
    empty_to_none,
    flush_pending_toasts,
    show_error_toast,
    show_success_toast,
)

from shared.config_params import (
    check_for_unknown_placeholders,
    substitute_dummy_values,
)
from shared.db.interface import archive_settings, create_settings
from shared.db.models import ProjectSettings, ProjectStatus, SettingsStatus
from shared.display_paths import DISPLAY_PATHS, get_display_settings_path
from shared.keys import (
    SOFTWARE_TYPE_TO_DEFAULT_RESOURCE_PARAMS,
    SOFTWARE_TYPE_TO_METRICS_TYPES,
    JobEngines,
    MetricsTypes,
    SoftwareTypes,
)
from shared.path_views import Locations
from shared.runners import RUNNERS, get_runner
from shared.validation import check_for_malicious_content

SHOW_RUNNER_SELECT = True

# every widget interaction reruns the page, so the reads below are cached rather than repeated.
# Writes clear the cache, the TTL only covers changes made elsewhere.
DB_CACHE_TTL_SECONDS = 60

_log(f"loading {__file__} {get_all_query_params()}")
# ########################################### PAGE HEADER

st.set_page_config(page_title="AlphaKraken: settings", layout="wide")

flush_pending_toasts()
show_sandbox_message()

st.markdown("# Manage settings")

# ########################################### LOGIC


@st.cache_data(ttl=DB_CACHE_TTL_SECONDS)
def get_settings_df() -> pd.DataFrame:
    """Get all settings entries."""
    return df_from_db_data(get_settings_data())


@st.cache_data(ttl=DB_CACHE_TTL_SECONDS)
def get_active_project_ids() -> set[str]:
    """Get the ids of all active projects."""
    return {p.id for p in get_project_data() if p.status == ProjectStatus.ACTIVE}


settings_df = get_settings_df()


# ########################################### DISPLAY

st.markdown("## Current settings")


@st.fragment
def display_settings(
    settings_df: pd.DataFrame,
    st_display: st.delta_generator.DeltaGenerator | Any = None,
) -> None:
    """Fragment to display settings in a table."""
    if st_display is None:
        st_display = st
    filtered_df, *_ = show_filter(
        settings_df, st_display=st_display, default_value="status=^active"
    )

    filtered_df = filtered_df.drop(columns=["_id"], errors="ignore").fillna("")

    if "config_params" in filtered_df.columns:
        filtered_df["config_params"] = filtered_df["config_params"].apply(
            lambda x: f"`{x}`" if x else x
        )

    st_display.table(
        filtered_df.style.apply(
            lambda row: [
                "color: lightgray"
                if row["status"] == SettingsStatus.INACTIVE
                else "background-color: white"
            ]
            * len(row),
            axis=1,
        )
    )

    st_display.markdown(
        "The files associated with settings are stored at "
        f"`{get_display_settings_path(SETTINGS_NAME_PLACEHOLDER)}/`"
    )


display_settings(settings_df)

# ########################################### CREATE NEW SETTINGS

c1, _ = st.columns([0.5, 0.5])
c1.markdown("## Create / update settings")


show_help(c1)


if not RUNNERS:
    c1.warning(
        "No runners are declared in `alphakraken.yaml`, so no settings can be created. Ask an admin."
    )
    st.stop()

# the software selection is built from all settings, so that archiving one does not drop its software
all_settings_df = settings_df

# only active settings beyond this point
if len(settings_df):
    settings_df = settings_df[settings_df["status"] == SettingsStatus.ACTIVE]

    # Get existing settings names for selectbox (outside form so it can trigger reruns)
    existing_settings_names = (
        sorted(set(settings_df["name"].tolist()))
        if not settings_df.empty and "name" in settings_df.columns
        else []
    )
else:
    existing_settings_names = []

CREATE_NEW_OPTION = "➕ Create new settings..."  # noqa: RUF001
ADD_NEW_SOFTWARE_OPTION = "➕ Add new software..."  # noqa: RUF001


settings_name_options = [CREATE_NEW_OPTION, *existing_settings_names]
selected_name_option = c1.selectbox(
    label="Action",
    options=settings_name_options,
    format_func=lambda x: x if x == CREATE_NEW_OPTION else f"🔄 Update '{x}'",
)

# Show info banner if existing settings selected and get latest version data for prefilling
prefill_data = defaultdict(lambda: "")  # TODO: normal dict?
current_version = -1
if selected_name_option != CREATE_NEW_OPTION:
    # Get the latest version of the selected settings
    latest_settings = (
        settings_df[settings_df["name"] == selected_name_option]
        .sort_values("version", ascending=False)
        .iloc[0]
    )
    current_version = int(latest_settings["version"])

    # Prepare prefill data from latest version
    prefill_data = {
        "description": str(latest_settings.get("description", "")),
        "software": str(latest_settings.get("software", "")),
        "software_type": str(latest_settings.get("software_type", "")),
        "runner_name": str(latest_settings.get("runner_name", "")),
        "fasta_file_name": str(latest_settings.get("fasta_file_name", "")),
        "speclib_file_name": str(latest_settings.get("speclib_file_name", "")),
        "config_file_name": str(latest_settings.get("config_file_name", "")),
        "config_params": str(latest_settings.get("config_params", "")),
        "metrics_type": str(latest_settings.get("metrics_type", "")),
        "slurm_cpus_per_task": latest_settings.get("slurm_cpus_per_task", ""),
        "slurm_mem": str(latest_settings.get("slurm_mem", "")),
        "slurm_time": str(latest_settings.get("slurm_time", "")),
        "num_threads": latest_settings.get("num_threads", ""),
    }
    # Conversion for legacy data. Remove once all is transferred.
    for k, v in prefill_data.items():
        if pd.isna(v):
            prefill_data[k] = ""


disable_software_type_selection = "software_type" in prefill_data
software_type_options = SoftwareTypes.get_values()
software_type_index = (
    software_type_options.index(prefill_data["software_type"])
    if prefill_data["software_type"] in software_type_options
    else 0
)
software_type = c1.selectbox(
    label="Type",
    options=software_type_options,
    index=software_type_index,
    disabled=disable_software_type_selection,
)

metrics_types_of_software = SOFTWARE_TYPE_TO_METRICS_TYPES[software_type]
# metrics can always be switched off, cf. MetricsTypes.CUSTOM
metrics_type_options = list(
    dict.fromkeys([*metrics_types_of_software, MetricsTypes.CUSTOM])
)
metrics_type_default = (
    prefill_data.get("metrics_type", "") or metrics_types_of_software[0]
)
metrics_type_index = (
    metrics_type_options.index(metrics_type_default)
    if metrics_type_default in metrics_type_options
    else 0
)
metrics_type = c1.selectbox(
    label="Metrics type",
    options=metrics_type_options,
    index=metrics_type_index,
    help=f"Which metrics to calculate, should typically match the software type. Set to `{MetricsTypes.CUSTOM}` to calculate none. Note: values from a `metrics.csv` in the output directory will be merged with those selected, overriding values on column name collision.",
)
if metrics_type == MetricsTypes.CUSTOM:
    c1.info(
        f"`{MetricsTypes.CUSTOM}` calculates no metrics, it only stores those the software "
        "reported itself in a `metrics.csv`, cf. `docs/customization.md`."
    )


runner_names = list(RUNNERS)
if SHOW_RUNNER_SELECT:
    prefilled_runner_name = prefill_data["runner_name"]
    if prefilled_runner_name and prefilled_runner_name not in runner_names:
        c1.warning(
            f"Runner `{prefilled_runner_name}` of the previous version is not declared in "
            f"`alphakraken.yaml` anymore, using `{runner_names[0]}`."
        )
    runner_index = (
        runner_names.index(prefilled_runner_name)
        if prefilled_runner_name in runner_names
        else 0
    )
    runner_name = c1.selectbox(
        label="Runner",
        options=runner_names,
        index=runner_index,
        help="Where the quanting job runs, cf. `runners` in `alphakraken.yaml`.",
    )
else:
    runner_name = runner_names[0]


form_items = build_form_items(software_type)

# outside the form, so that the required files and software below follow the input without a submit
# Show input field for new name or use selected name
if selected_name_option == CREATE_NEW_OPTION:
    name = c1.text_input(
        label=form_items["name"]["label"],
        max_chars=form_items["name"]["max_chars"],
        placeholder=form_items["name"]["placeholder"],
        help=form_items["name"]["help"],
    )
else:
    name = selected_name_option
    c1.text(f"Settings name: {name}")

description = c1.text_area(
    **form_items["description"], value=prefill_data["description"]
)

settings_using_software = get_settings_using_software(
    all_settings_df, software_type, runner_name
)
# the frame is sorted by creation date descending, cf. df_from_db_data
used_software = list(dict.fromkeys(settings_using_software["software"].dropna()))
prefilled_software = prefill_data["software"]

# a prefilled software the selected runner has never run stays selectable, it is not swapped silently
software_options = used_software + (
    [prefilled_software]
    if prefilled_software and prefilled_software not in used_software
    else []
)

selected_software_option = c1.selectbox(
    label=form_items["software"]["label"],
    options=[*software_options, ADD_NEW_SOFTWARE_OPTION],
    # the prefilled one, else the most recently used one, else the "add new" entry
    index=software_options.index(prefilled_software) if prefilled_software else 0,
    help=form_items["software"]["help"],
)
if selected_software_option == ADD_NEW_SOFTWARE_OPTION:
    software = c1.text_input(
        label="New software",
        max_chars=form_items["software"]["max_chars"],
        placeholder=form_items["software"]["placeholder"],
        help=form_items["software"]["help"],
    )
    c1.warning(
        "No settings use this software yet: an administrator needs to make it available first, "
        "otherwise the quanting jobs of these settings will fail.",
        icon="⚠️",
    )
else:
    software = selected_software_option

software_used_by = [
    f"`{row['name']}` v{int(row['version'])}"
    for _, row in settings_using_software.iterrows()
    if row["software"] == software
]

fasta_file_name = (
    c1.text_input(
        **form_items["fasta_file_name"], value=prefill_data["fasta_file_name"]
    )
    if "fasta_file_name" in form_items
    else None
)
speclib_file_name = (
    c1.text_input(
        **form_items["speclib_file_name"], value=prefill_data["speclib_file_name"]
    )
    if "speclib_file_name" in form_items
    else None
)

config_file_name = (
    c1.text_input(
        **form_items["config_file_name"], value=prefill_data["config_file_name"]
    )
    if "config_file_name" in form_items
    else None
)

config_params = (
    c1.text_area(
        **form_items["config_params"],
        value=prefill_data.get("config_params", ""),
    )
    if "config_params" in form_items
    else None
)

if "config_params" in form_items:
    show_placeholder_info(c1, runner_name)

if software_type == SoftwareTypes.CUSTOM:
    show_software_examples(c1)

c1.write(r"\* Required fields")
if software_type == SoftwareTypes.ALPHADIA:
    c1.write(r"\** At least one of the two must be given")

with c1.expander("Resource parameters"):
    st.info(
        "Enables setting the resources. Some values are only relevant for Slurm and/or for alphadia/custom."
    )
    resource_params_defaults = SOFTWARE_TYPE_TO_DEFAULT_RESOURCE_PARAMS[software_type]

    slurm_cpus_per_task = st.number_input(
        label="CPUs per task [Slurm only]",
        min_value=1,
        value=int(
            prefill_data["slurm_cpus_per_task"]
            or resource_params_defaults.slurm_cpus_per_task
        ),
        help="Mapped to --cpus-per-task",
    )
    slurm_mem = st.text_input(
        label="Memory (e.g. '62G') [Slurm only]",
        max_chars=16,
        value=prefill_data["slurm_mem"] or resource_params_defaults.slurm_mem,
        help="Mapped to --mem",
    )
    slurm_time = st.text_input(
        label="Time limit (HH:MM:SS) [Slurm only]",
        max_chars=16,
        value=prefill_data["slurm_time"] or resource_params_defaults.slurm_time,
        help="Mapped to --time",
    )
    num_threads = st.number_input(
        label="Number of threads [alphadia and custom only]",
        min_value=1,
        value=int(prefill_data["num_threads"] or resource_params_defaults.num_threads),
        help="Use for 'alphadia' and 'custom' (through {{NUM_THREADS}} placeholder)",
    )

c1.markdown("### Required files and software")
settings_name_clean = empty_to_none(name)
settings_folder = get_display_settings_path(
    settings_name_clean or SETTINGS_NAME_PLACEHOLDER
)

referenced_files = [
    file_name
    for file_name in (fasta_file_name, speclib_file_name, config_file_name)
    if empty_to_none(file_name)
]
if referenced_files:
    c1.markdown(
        f"Make sure you uploaded these files to `{settings_folder}/`:\n"
        + "\n".join(f"- `{file_name}`" for file_name in referenced_files)
    )

if software_used_by:
    c1.markdown(
        f"The software `{software}` is already used by {', '.join(software_used_by)}."
    )
elif empty_to_none(software):
    # `software` is a path below the software folder only for the non-containerized non-alphadia case
    if software_type == SoftwareTypes.ALPHADIA:
        software_hint = f"the Conda environment `{software}`"
    elif get_runner(runner_name).engine == JobEngines.DOCKER:
        software_hint = f"the docker image `{software}` on the worker host"
    else:
        software_hint = f"the software `{DISPLAY_PATHS[Locations.SOFTWARE]}/{software}`"
    c1.markdown(
        f"Make sure {software_hint} is available, ask an administrator if in doubt."
    )

upload_checkbox = (
    c1.checkbox(
        "I have uploaded all referenced files to this folder and checked the software is available.",
        value=False,
    )
    if referenced_files
    else True
)

is_update = selected_name_option != CREATE_NEW_OPTION
if is_update:
    c1.info(
        f"This will create a new version ({current_version + 1}) of the existing settings '{selected_name_option}'. "
        f"Projects always reference a specific version of settings, so existing projects using '{selected_name_option}' version {current_version} will not be affected. "
        "Make sure to update (all or selected) projects to use the new version after creating it.",
        icon="ℹ️",  # noqa: RUF001
    )
    archive_previous = c1.checkbox(
        f"Archive previous version ({current_version}) after creating the new version",
        value=False,
    )
submit_label = f"Update settings '{name}'" if is_update else "Create settings"
submit = c1.button(
    submit_label,
    disabled=DISABLE_WRITE,
    help="Temporarily disabled." if DISABLE_WRITE else "",
)


if submit:
    validation_errors = []
    for to_validate in [
        fasta_file_name,
        speclib_file_name,
        software,
        config_file_name,
    ]:
        if to_validate:
            validation_errors.extend(check_for_malicious_content(to_validate))
    validation_errors.extend(
        check_runner_supports_software_type(runner_name, software_type)
    )
    if config_params:
        # TODO: warn on bare (RAW_FILE_PATH) and half-open ({{RAW_FILE_PATH) placeholders,
        # they currently pass validation and fail silently at runtime
        placeholder_errors = check_for_unknown_placeholders(config_params)
        validation_errors.extend(
            placeholder_errors
            or check_for_malicious_content(
                substitute_dummy_values(config_params), allow_spaces=True
            )
        )
    if slurm_mem:
        validation_errors.extend(check_for_malicious_content(slurm_mem))
        if not re.match(r"^\d+[KMGT]$", slurm_mem):
            validation_errors.append(
                "Memory must be a number followed by a unit (K, M, G, or T), e.g. '62G'."
            )
    if slurm_time and not re.match(r"^\d{2}:\d{2}:\d{2}$", slurm_time):
        validation_errors.append("SLURM time must be in HH:MM:SS format.")

    if software_type == SoftwareTypes.ALPHADIA and (
        empty_to_none(fasta_file_name) is None
        and empty_to_none(speclib_file_name) is None
    ):
        validation_errors.append(
            "At least one of the fasta and speclib file names must be given."
        )

    try:
        if validation_errors:
            errors_str = "\n- ".join(validation_errors)
            raise ValueError(
                f"Found {len(validation_errors)} Input validation error:\n- {errors_str}"
            )

        if not upload_checkbox:
            raise ValueError(
                "Please upload the files to the respective folders on the pool file system and check the respective box."
            )

        create_settings(
            name=empty_to_none(name),
            description=empty_to_none(description),
            fasta_file_name=fasta_file_name,
            speclib_file_name=speclib_file_name,
            config_file_name=config_file_name,
            config_params=config_params,
            software_type=empty_to_none(software_type),
            software=empty_to_none(software),
            runner_name=runner_name,
            metrics_type=metrics_type,
            slurm_cpus_per_task=slurm_cpus_per_task,
            slurm_mem=empty_to_none(slurm_mem),
            slurm_time=empty_to_none(slurm_time),
            num_threads=num_threads,
        )
    except Exception as e:  # noqa: BLE001
        show_error_toast(str(e))
    else:
        if is_update and archive_previous:
            archive_settings(latest_settings["_id"])
        get_settings_df.clear()
        show_success_toast(
            f"Created new settings '{name}'. Assign it to projects on the Projects page."
        )


# ########################################### ARCHIVE SETTINGS

c1.markdown("## Archive settings")

if len(settings_df):
    active_settings_df = settings_df[
        settings_df["status"] == SettingsStatus.ACTIVE
    ].sort_values(["name", "version"], ascending=[True, False])
else:
    active_settings_df = pd.DataFrame()

if active_settings_df.empty:
    c1.info("No active settings to archive.")
else:
    with c1.expander("Show settings to archive .."):
        st.warning(
            "Archived settings can no longer be assigned to projects or updated."
        )

        # Map settings ID -> list of active project IDs
        all_ps = ProjectSettings.objects.all()
        active_project_ids = get_active_project_ids()
        assigned_projects: dict[str, list[str]] = defaultdict(list)
        for ps in all_ps:
            project_id = str(ps.project.id)
            if project_id in active_project_ids:
                assigned_projects[str(ps.settings.id)].append(project_id)

        for _, row in active_settings_df.iterrows():
            col_btn, col_info = st.columns([0.2, 0.8])
            projects = sorted(set(assigned_projects.get(str(row["_id"]), [])))
            projects_str = ", ".join(projects) if projects else "none"
            col_info.write(
                f"'{row['name']}' version {int(row['version'])} (type: `{row.get('software_type', '')}`, executable: `{row.get('software', '')}`, description: `{row.get('description', '')}`) — assigned to projects: {projects_str}"
            )
            if col_btn.button(
                "Archive",
                key=f"archive_{row['_id']}",
                disabled=DISABLE_WRITE,
                icon=":material/archive:",
            ):
                try:
                    # TODO: consider showing a warning in the webapp when archived settings are still assigned.
                    archive_settings(row["_id"])
                    get_settings_df.clear()
                    show_success_toast(
                        f"Archived settings '{row['name']}' version {int(row['version'])}."
                    )
                except Exception as e:  # noqa: BLE001
                    show_error_toast(str(e))
