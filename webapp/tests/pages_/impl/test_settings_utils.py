"""Tests for the settings page utilities."""

import pandas as pd
from pages_.impl.settings_utils import build_form_items, get_settings_using_software

from shared.keys import SoftwareTypes


def _settings_df() -> pd.DataFrame:
    """Get settings of mixed software type and runner, youngest first, as df_from_db_data sorts them."""
    return pd.DataFrame(
        {
            "name": ["young", "old", "other_type", "other_runner"],
            "version": [2, 1, 1, 1],
            "software": ["adia-2", "adia-1", "run_msqc.sh", "adia-on-docker"],
            "software_type": ["alphadia", "alphadia", "msqc", "alphadia"],
            "runner_name": ["slurm", "slurm", "slurm", "docker"],
        },
    )


def test_get_settings_using_software_keeps_type_runner_and_order() -> None:
    """Test that only the settings of the given type and runner are returned, order kept."""
    result = get_settings_using_software(_settings_df(), "alphadia", "slurm")

    assert result["name"].tolist() == ["young", "old"]


def test_get_settings_using_software_without_matches_returns_empty() -> None:
    """Test that a type and runner combination nothing uses yields no settings."""
    result = get_settings_using_software(_settings_df(), "skyline", "slurm")

    assert result.empty


def test_get_settings_using_software_tolerates_an_empty_frame() -> None:
    """Test that a deployment without any settings does not raise."""
    assert get_settings_using_software(pd.DataFrame(), "alphadia", "slurm").empty


def test_get_settings_using_software_tolerates_missing_columns() -> None:
    """Test that legacy settings without the required columns do not raise."""
    legacy_df = pd.DataFrame({"name": ["legacy"], "version": [1]})

    assert get_settings_using_software(legacy_df, "alphadia", "slurm").empty


def test_build_form_items_gives_alphadia_its_input_files() -> None:
    """Test that alphadia offers the fasta, speclib and config file fields."""
    form_items = build_form_items(SoftwareTypes.ALPHADIA)

    assert {"fasta_file_name", "speclib_file_name", "config_file_name"} <= set(
        form_items
    )
    assert "config_params" not in form_items


def test_build_form_items_gives_custom_software_its_config_params() -> None:
    """Test that custom software offers the config parameters instead of input files."""
    form_items = build_form_items(SoftwareTypes.CUSTOM)

    assert "config_params" in form_items
    assert "fasta_file_name" not in form_items


def test_build_form_items_always_gives_name_description_and_software() -> None:
    """Test that every software type asks for the fields the DB requires."""
    for software_type in SoftwareTypes.get_values():
        assert {"name", "description", "software"} <= set(
            build_form_items(software_type)
        ), software_type
