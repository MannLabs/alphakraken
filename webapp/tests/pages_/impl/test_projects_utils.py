"""Tests for the projects page utilities."""

from unittest.mock import MagicMock, patch

from pages_.impl.projects_utils import (
    SAME_SOFTWARE_TYPE_WARNING,
    get_resolved_settings_df,
)

from shared.domain.settings import Settings
from shared.tests.factories import project_settings, settings

_INSTRUMENTS_CONFIG = {"instr1": {"type": "thermo"}, "instr2": {"type": "bruker"}}


def _settings(name: str, software_type: str) -> Settings:
    return settings(id=f"{name}_v1", name=name, version=1, software_type=software_type)


@patch("shared.db.project_settings.find", return_value=[])
@patch("pages_.impl.projects_utils.resolve_scoped_settings")
def test_get_resolved_settings_df_lists_all_resolved_settings_per_instrument(
    mock_resolve: MagicMock,
    mock_find: MagicMock,
) -> None:
    """Test that one row per resolved settings and a placeholder row for empty instruments is built."""
    mock_resolve.side_effect = lambda *_, instrument_id, **__: (
        [_settings("a", "alphadia"), _settings("b", "custom")]
        if instrument_id == "instr1"
        else []
    )

    df = get_resolved_settings_df("P1", ["instr1", "instr2"], _INSTRUMENTS_CONFIG)

    assert df["instrument"].tolist() == ["instr1", "instr1", "instr2"]
    assert df["settings"].tolist() == [
        "a version 1 (alphadia)",
        "b version 1 (custom)",
        "--",
    ]
    assert mock_resolve.call_args_list[1].kwargs["instrument_type"] == "bruker"
    mock_find.assert_called_once_with(["P1"])


@patch("shared.db.project_settings.find", return_value=[])
@patch("pages_.impl.projects_utils.resolve_scoped_settings")
def test_get_resolved_settings_df_warns_on_same_software_type(
    mock_resolve: MagicMock,
    mock_find: MagicMock,  # noqa: ARG001
) -> None:
    """Test that two settings of the same software_type on one instrument are flagged."""
    mock_resolve.return_value = [
        _settings("a", "custom"),
        _settings("b", "custom"),
        _settings("c", "alphadia"),
    ]

    df = get_resolved_settings_df("P1", ["instr1"], _INSTRUMENTS_CONFIG)

    flagged = [SAME_SOFTWARE_TYPE_WARNING in s for s in df["settings"]]
    assert flagged == [True, True, False]


@patch("shared.db.project_settings.find")
def test_get_resolved_settings_df_annotates_file_name_filters(
    mock_find: MagicMock,
) -> None:
    """Test that the filters of the matching assignment are shown, those of a non-matching one not."""
    a = _settings("a", "alphadia")
    ps_match = project_settings(
        settings=a,
        scopes=["thermo"],
        raw_file_id_filter=["PP", "QQ"],
        raw_file_id_exclude_filter=["blank"],
    )
    ps_other_instrument = project_settings(
        settings=a, scopes=["bruker"], raw_file_id_filter=["ZZ"]
    )
    mock_find.return_value = [ps_match, ps_other_instrument]

    df = get_resolved_settings_df("P1", ["instr1"], _INSTRUMENTS_CONFIG)

    assert df["settings"].tolist() == [
        "a version 1 (alphadia) (only for files containing: PP, QQ; not for files containing: blank)"
    ]
