"""Utility functions for the projects page."""

from collections import Counter

import pandas as pd

from shared.db.interface import get_project_settings
from shared.db.models import ProjectSettings, Settings
from shared.settings_scope_resolver import resolve_scoped_settings
from shared.yamlsettings import YamlKeys

SAME_SOFTWARE_TYPE_WARNING = "⚠️ same software type as another settings on this instrument. Check this is intended and/or make sure the file name filters separate them."


def get_resolved_settings_df(
    selected_project_id: str, instrument_ids: list[str], instruments_config: dict
) -> pd.DataFrame:
    """Build a DataFrame showing resolved settings per instrument.

    File name filters are ignored here, so every settings that could apply is shown.
    """
    all_ps = get_project_settings(selected_project_id)
    rows = []
    for instrument_id in instrument_ids:
        instrument_type = instruments_config.get(instrument_id, {}).get(
            YamlKeys.TYPE, ""
        )
        resolved = resolve_scoped_settings(
            all_ps, instrument_id=instrument_id, instrument_type=instrument_type
        )
        if not resolved:
            rows.append(
                {
                    "instrument": instrument_id,
                    "settings": "--",
                    "software": "--",
                    "details": "",
                }
            )
            continue

        software_type_counts = Counter(s.software_type for s in resolved)
        for s in resolved:
            filter_annotation = _describe_filters(
                s, all_ps, instrument_id=instrument_id, instrument_type=instrument_type
            )
            detail_parts = [
                f"description: {s.description}" if s.description else None,
                f"config_file: {s.config_file_name}" if s.config_file_name else None,
                f"fasta: {s.fasta_file_name}" if s.fasta_file_name else None,
                f"speclib: {s.speclib_file_name}" if s.speclib_file_name else None,
                f"config_params: {s.config_params}" if s.config_params else None,
            ]
            warning = (
                f" {SAME_SOFTWARE_TYPE_WARNING}"
                if software_type_counts[s.software_type] > 1
                else ""
            )
            rows.append(
                {
                    "instrument": instrument_id,
                    "settings": f"{s.name} version {s.version} ({s.software_type}){filter_annotation}{warning}",
                    "software": s.software,
                    "details": " | ".join(p for p in detail_parts if p),
                }
            )
    return pd.DataFrame(rows)


def _describe_filters(
    settings: Settings,
    all_ps: list[ProjectSettings],
    *,
    instrument_id: str,
    instrument_type: str,
) -> str:
    """Describe the file-name filters of the assignments that resolve `settings` for the instrument."""
    parts = []
    for ps in all_ps:
        if ps.settings.id != settings.id or not resolve_scoped_settings(  # type: ignore[unresolved-attribute]
            [ps], instrument_id=instrument_id, instrument_type=instrument_type
        ):
            continue
        if ps.raw_file_id_filter:
            parts.append(
                f"only for files containing: {', '.join(ps.raw_file_id_filter)}"
            )
        if ps.raw_file_id_exclude_filter:
            parts.append(
                f"not for files containing: {', '.join(ps.raw_file_id_exclude_filter)}"
            )
    return f" ({'; '.join(parts)})" if parts else ""
