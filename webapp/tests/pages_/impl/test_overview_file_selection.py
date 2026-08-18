"""Tests for the overview_file_selection module."""

from unittest.mock import MagicMock, patch

import pandas as pd
from pages_.impl.overview_file_selection import _show_output_folders


@patch("pages_.impl.overview_file_selection.st")
@patch("pages_.impl.overview_file_selection.get_output_folders")
def test_show_output_folders_collapses_repeated_runs_of_one_setting(
    mock_get_output_folders: MagicMock,
    mock_st: MagicMock,
) -> None:
    """Test that multiple runs of the same settings on a raw file collapse to one folder."""
    # two runs of the same setting on f1 (identical output_path) plus a run on f2
    mock_get_output_folders.return_value = pd.DataFrame(
        {
            "raw_file_id": ["f1", "f1", "f2"],
            "settings_name": ["s1", "s1", "s1"],
            "settings_version": [1, 1, 1],
            "type": ["alphadia", "alphadia", "alphadia"],
            "output_path": ["/out/f1/alphadia", "/out/f1/alphadia", "/out/f2/alphadia"],
        }
    )
    mock_st.expander.return_value.__enter__ = MagicMock()
    mock_st.expander.return_value.__exit__ = MagicMock()

    # when
    _show_output_folders(["f1", "f2"])

    # then: the duplicated f1 folder is shown only once
    mock_st.expander.assert_called_once_with(
        "**s1** (v1, alphadia) — 2 folders", expanded=False
    )
    mock_st.code.assert_called_once_with("/out/f1/alphadia\n/out/f2/alphadia")


@patch("pages_.impl.overview_file_selection.st")
@patch("pages_.impl.overview_file_selection.get_output_folders")
def test_show_output_folders_warns_on_raw_file_with_multiple_folders(
    mock_get_output_folders: MagicMock,
    mock_st: MagicMock,
) -> None:
    """Test that a raw file with more than one output folder in a group is flagged."""
    # f1 ran twice with the same setting but wrote to two distinct folders
    mock_get_output_folders.return_value = pd.DataFrame(
        {
            "raw_file_id": ["f1", "f1", "f2"],
            "settings_name": ["s1", "s1", "s1"],
            "settings_version": [1, 1, 1],
            "type": ["alphadia", "alphadia", "alphadia"],
            "output_path": ["/out/f1/a", "/out/f1/b", "/out/f2/a"],
        }
    )
    mock_st.expander.return_value.__enter__ = MagicMock()
    mock_st.expander.return_value.__exit__ = MagicMock()

    # when
    _show_output_folders(["f1", "f2"])

    # then: f1 (2 folders) is flagged, f2 is not
    mock_st.warning.assert_called_once()
    warning_message = mock_st.warning.call_args[0][0]
    assert "f1 (2 folders)" in warning_message
    assert "f2" not in warning_message


@patch("pages_.impl.overview_file_selection.st")
@patch("pages_.impl.overview_file_selection.get_output_folders")
def test_show_output_folders_no_warning_when_folders_unique_per_raw_file(
    mock_get_output_folders: MagicMock,
    mock_st: MagicMock,
) -> None:
    """Test that no warning is shown when each raw file maps to a single folder per group."""
    mock_get_output_folders.return_value = pd.DataFrame(
        {
            "raw_file_id": ["f1", "f2"],
            "settings_name": ["s1", "s1"],
            "settings_version": [1, 1],
            "type": ["alphadia", "alphadia"],
            "output_path": ["/out/f1/a", "/out/f2/a"],
        }
    )
    mock_st.expander.return_value.__enter__ = MagicMock()
    mock_st.expander.return_value.__exit__ = MagicMock()

    # when
    _show_output_folders(["f1", "f2"])

    # then
    mock_st.warning.assert_not_called()


@patch("pages_.impl.overview_file_selection.st")
@patch("pages_.impl.overview_file_selection.get_output_folders")
def test_show_output_folders_no_folders(
    mock_get_output_folders: MagicMock,
    mock_st: MagicMock,
) -> None:
    """Test that an info message is shown when no output folders exist for the selection."""
    mock_get_output_folders.return_value = pd.DataFrame(
        columns=[
            "raw_file_id",
            "settings_name",
            "settings_version",
            "type",
            "output_path",
        ]
    )

    # when
    _show_output_folders(["f1"])

    # then
    mock_st.info.assert_called_once()
    mock_st.expander.assert_not_called()
