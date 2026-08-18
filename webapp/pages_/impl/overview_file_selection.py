"""File selection functionality for the overview page."""

import pandas as pd
import streamlit as st
import streamlit.delta_generator
from service.components import get_full_backup_path
from service.db import get_full_raw_file_data, get_output_folders
from service.utils import METRICS_TYPE_SEPARATOR

# name of the checkbox column in the file selection editor
SELECTED_COLUMN = "selected"


def _show_file_paths(file_ids: list, prefix: str) -> None:
    """Show the full backup paths for the given raw file ids."""
    full_info_df = get_full_raw_file_data(file_ids)
    file_paths, is_multiple_types, errors = get_full_backup_path(full_info_df)

    with st.expander(f"Found {len(file_paths)} items:", expanded=True):
        if is_multiple_types:
            st.warning(
                "Warning: more than one instrument type found, please check your selection!"
            )
        if errors:
            errors_str = "\n- ".join(errors)
            st.warning(
                f"The following {len(errors)} files have been excluded from the selection:\n- {errors_str}"
            )

        st.write("One-line format:")
        file_paths_pretty_one_line = " ".join(file_paths)
        st.code(f"{file_paths_pretty_one_line}")

        st.write("Multi-line format:")

        file_paths_pretty = f"\n{prefix}".join(file_paths)
        file_paths_multi_line = f"{prefix}{file_paths_pretty}"
        st.code(file_paths_multi_line)
        st.download_button(
            label="⬇️ Download as txt",
            data=file_paths_multi_line,
            file_name="AlphaKraken_file_paths.txt",
            mime="text/plain",
        )


def _show_file_paths_controls(
    file_ids: list,
    *,
    checkbox_display: st.delta_generator.DeltaGenerator,
    button_display: st.delta_generator.DeltaGenerator,
    button_visible: bool,
    button_help: str,
) -> None:
    """Show the AlphaDIA-prefix checkbox and, if `button_visible`, the 'Show file paths' button."""
    prefix = (
        " - "
        if checkbox_display.checkbox(
            "AlphaDIA-compatible prefix",
            help="Whether the Multi-line format should carry a hyphen as prefix",
        )
        else ""
    )
    if button_visible and button_display.button(
        "🔗 Show file paths",
        help=button_help,
    ):
        _show_file_paths(file_ids, prefix)


def _show_output_folders(file_ids: list) -> None:
    """Show the output folders for the given raw file ids, grouped by settings and type.

    Multiple runs of the same settings on a raw file write to the same output folder,
    so identical folders within a settings/type group are collapsed to a single entry.
    If a raw file still maps to more than one distinct output folder within a group, this
    is flagged as a warning.
    """
    output_folders_df = get_output_folders(file_ids)
    output_folders_df = output_folders_df[
        output_folders_df["output_path"].notna()
    ].drop_duplicates(
        subset=["settings_name", "settings_version", "type", "output_path"]
    )

    if output_folders_df.empty:
        st.info("No output folders found for the selection.")
        return


    for (
        settings_name,
        settings_version,
        metrics_type,
    ), group in output_folders_df.groupby(
        ["settings_name", "settings_version", "type"]
    ):
        output_paths = group["output_path"].tolist()

        with st.expander(f"**{settings_name}** (v{settings_version}, {metrics_type}) — {len(output_paths)} folders", expanded=False):

            raw_file_folder_counts = group.groupby("raw_file_id").size()
            multi_folder_raw_files = raw_file_folder_counts[raw_file_folder_counts > 1]
            if not multi_folder_raw_files.empty:
                multi_folder_str = "\n- ".join(
                    f"{raw_file_id} ({count} folders)"
                    for raw_file_id, count in multi_folder_raw_files.items()
                )
                st.warning(
                    f"Warning: the following {len(multi_folder_raw_files)} raw files have "
                    f"more than one output folder in this group:\n- {multi_folder_str}"
                )
            
            output_paths_str = "\n".join(output_paths)
            st.code(output_paths_str)
            st.download_button(
                label="⬇️ Download as txt",
                data=output_paths_str,
                file_name=f"AlphaKraken_output_folders_{settings_name}-v{settings_version}_{metrics_type}.txt",
                mime="text/plain",
                key=f"download_output_folders_{settings_name}_{settings_version}_{metrics_type}",
            )


def _show_output_folders_button(
    file_ids: list,
    *,
    button_display: st.delta_generator.DeltaGenerator,
    button_visible: bool,
    button_help: str,
) -> None:
    """Show, if `button_visible`, the 'Show output folders' button and its output when clicked."""
    if button_visible and button_display.button(
        "📁 Show output folders",
        help=button_help,
    ):
        _show_output_folders(file_ids)


def _show_file_selection(df: pd.DataFrame, max_table_len: int) -> None:
    """Show a checkbox list of files with basic data and buttons to export the ticked selection."""
    df_to_show = df.head(max_table_len)

    proteins_columns = [
        c for c in df_to_show.columns if c.endswith(f"{METRICS_TYPE_SEPARATOR}proteins")
    ]
    precursors_columns = [
        c
        for c in df_to_show.columns
        if c.endswith(f"{METRICS_TYPE_SEPARATOR}precursors")
    ]
    data_columns = [
        c
        for c in ["file_created", "size_gb", *proteins_columns, *precursors_columns]
        if c in df_to_show.columns
    ]

    selection_df = df_to_show[data_columns].copy()
    selection_df.insert(0, SELECTED_COLUMN, value=True)

    edited_df = st.data_editor(
        selection_df,
        column_order=[SELECTED_COLUMN, *data_columns],
        column_config={
            SELECTED_COLUMN: st.column_config.CheckboxColumn("✓", default=True),
            "_index": {"label": "raw_file_id"},
        },
        disabled=data_columns,
        hide_index=False,
    )

    selected_ids = edited_df[edited_df[SELECTED_COLUMN]].index.tolist()
    st.write(f"{len(selected_ids)} / {len(edited_df)} files ticked.")

    c1, c2, c3, c4, _ = st.columns([0.15, 0.15, 0.15, 0.15, 0.40])
    _show_file_paths_controls(
        selected_ids,
        checkbox_display=c4,
        button_display=c1,
        button_visible=not df_to_show.empty,
        button_help="For the ticked files, show all file paths on the backup for conveniently copying them manually to another location.",
    )

    if c2.button(
        "🔤 Show file names",
        help="For the ticked files, show the file names (one per line).",
    ):
        with st.expander(f"{len(selected_ids)} file names:", expanded=True):

            file_names_str = "\n".join(str(file_id) for file_id in selected_ids)
            st.code(file_names_str)
            st.download_button(
                label="⬇️ Download as txt",
                data=file_names_str,
                file_name="AlphaKraken_file_names.txt",
                mime="text/plain",
            )

    _show_output_folders_button(
        selected_ids,
        button_display=c3,
        button_visible=not df_to_show.empty,
        button_help="For the ticked files, show the output folders where results are stored, grouped by settings and type.",
    )
