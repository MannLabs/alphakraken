"""Utility functions for the projects page."""

from typing import Any

import pandas as pd

from shared.db.interface import get_project_settings
from shared.settings_scope_resolver import resolve_scoped_settings
from shared.yamlsettings import YamlKeys


def get_resolved_settings_df(
    selected_project_id: str, instrument_ids: list[str], instruments_config: dict
) -> pd.DataFrame:
    """Build a DataFrame showing resolved settings per instrument.

    Groups assignments by raw_file_id_filter and resolves each group
    independently, so both base and filter-specific settings appear.
    """
    all_ps = get_project_settings(selected_project_id)
    groups: dict[str, list] = {}
    for ps in all_ps:
        groups.setdefault(ps.raw_file_id_filter, []).append(ps)
    rows = []
    for instrument_id in instrument_ids:
        instrument_type = instruments_config.get(instrument_id, {}).get(
            YamlKeys.TYPE, ""
        )
        resolved_entries: list[tuple[Any, str]] = []
        for filter_val, ps_group in sorted(groups.items()):
            for s in resolve_scoped_settings(
                ps_group,
                instrument_id=instrument_id,
                instrument_type=instrument_type,
            ):
                resolved_entries.append((s, filter_val))  # noqa: PERF401
        if not resolved_entries:
            rows.append(
                {
                    "instrument": instrument_id,
                    "settings": "--",
                    "software": "--",
                    "details": "",
                }
            )
            continue
        for s, filter_annotation in resolved_entries:
            detail_parts = [
                f"description: {s.description}" if s.description else None,
                f"config_file: {s.config_file_name}" if s.config_file_name else None,
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
    return pd.DataFrame(rows)
