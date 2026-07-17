"""File selection functionality for the overview page."""

import pandas as pd
import streamlit as st
from service.components import get_full_backup_path
from service.db import get_full_raw_file_data
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
        st.code(f"{prefix}{file_paths_pretty}")


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

    c1, c2, c3, _ = st.columns([0.15, 0.15, 0.15, 0.55])
    prefix = (
        " - "
        if c3.checkbox(
            "AlphaDIA-compatible prefix",
            help="Whether the Multi-line format should carry a hyphen as prefix",
        )
        else ""
    )

    if c1.button(
        "🔗 Show file paths for selection",
        help="For the ticked files, show all file paths on the backup for conveniently copying them manually to another location.",
    ):
        _show_file_paths(selected_ids, prefix)

    if c2.button(
        "🔤 Show file names",
        help="For the ticked files, show the file names (one per line).",
    ):
        with st.expander(f"{len(selected_ids)} file names:", expanded=True):
            st.code("\n".join(str(file_id) for file_id in selected_ids))
