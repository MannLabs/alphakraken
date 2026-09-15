"""Tests for the projects page utilities."""

from unittest.mock import MagicMock, patch

from pages_.impl.projects_utils import (
    SAME_SOFTWARE_TYPE_WARNING,
    get_resolved_settings_df,
)

_INSTRUMENTS_CONFIG = {"instr1": {"type": "thermo"}, "instr2": {"type": "bruker"}}


def _settings(name: str, software_type: str) -> MagicMock:
    s = MagicMock(
        version=1,
        software_type=software_type,
        software="sw",
        description="",
        config_file_name="",
        fasta_file_name="",
        speclib_file_name="",
        config_params="",
    )
    s.name = name
    return s


@patch("pages_.impl.projects_utils.get_project_settings", return_value=[])
@patch("pages_.impl.projects_utils.resolve_scoped_settings")
def test_get_resolved_settings_df_lists_all_resolved_settings_per_instrument(
    mock_resolve: MagicMock,
    mock_get_ps: MagicMock,  # noqa: ARG001
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


@patch("pages_.impl.projects_utils.get_project_settings", return_value=[])
@patch("pages_.impl.projects_utils.resolve_scoped_settings")
def test_get_resolved_settings_df_warns_on_same_software_type(
    mock_resolve: MagicMock,
    mock_get_ps: MagicMock,  # noqa: ARG001
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
