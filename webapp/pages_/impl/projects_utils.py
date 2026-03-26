"""Utility functions for the projects page."""

import pandas as pd

from shared.db.interface import get_project_settings
from shared.scope import resolve_scoped_settings
from shared.yamlsettings import YamlKeys


def get_resolved_settings_df(
    selected_project_id: str, instrument_ids: list[str], instruments_config: dict
) -> pd.DataFrame:
    """TODO."""
    current_ps_for_table = get_project_settings(selected_project_id)
    unfiltered_ps = [ps for ps in current_ps_for_table if not ps.raw_file_id_filter]
    unique_filters = sorted(
        {ps.raw_file_id_filter for ps in current_ps_for_table if ps.raw_file_id_filter}
    )
    rows = []
    for instrument_id in instrument_ids:
        instrument_type = instruments_config.get(instrument_id, {}).get(
            YamlKeys.TYPE, ""
        )
        base_resolved = resolve_scoped_settings(
            unfiltered_ps,
            instrument_id=instrument_id,
            instrument_type=instrument_type,
        )
        resolved_entries = [(s, "") for s in base_resolved]
        for filter_val in unique_filters:
            filtered_ps = [
                ps for ps in current_ps_for_table if ps.raw_file_id_filter == filter_val
            ]
            filtered_resolved = resolve_scoped_settings(
                filtered_ps,
                instrument_id=instrument_id,
                instrument_type=instrument_type,
                raw_file_id=filter_val,
            )
            for s in filtered_resolved:
                resolved_entries.append((s, filter_val))  # noqa: PERF401
        if resolved_entries:
            for s, filter_annotation in resolved_entries:
                detail_parts = [
                    f"description: {s.description}" if s.description else None,
                    f"config_file: {s.config_file_name}"
                    if s.config_file_name
                    else None,
                    f"fasta: {s.fasta_file_name}" if s.fasta_file_name else None,
                    f"speclib: {s.speclib_file_name}" if s.speclib_file_name else None,
                    f"config_params: {s.config_params}" if s.config_params else None,
                ]
                annotation = (
                    f" (only for files containing: {filter_annotation})"
                    if filter_annotation
                    else ""
                )
                rows.append(
                    {
                        "instrument": instrument_id,
                        "settings": f"{s.name} version {s.version} ({s.software_type}){annotation}",
                        "software": s.software,
                        "details": " | ".join(p for p in detail_parts if p),
                    }
                )
        else:
            rows.append(
                {
                    "instrument": instrument_id,
                    "settings": "--",
                    "software": "--",
                    "details": "",
                }
            )
    return pd.DataFrame(rows)
