"""Tests for the settings page."""

from datetime import datetime
from pathlib import Path
from unittest import skip
from unittest.mock import MagicMock, patch

import pandas as pd
import pytz
from pandas import Timestamp
from streamlit.testing.v1 import AppTest

APP_FOLDER = Path(__file__).parent / Path("../../")


@patch("service.db.get_project_data")
@patch("service.db.get_settings_data")
@patch("service.db.df_from_db_data")
def test_settings(
    mock_df: MagicMock, mock_get: MagicMock, mock_project_get: MagicMock
) -> None:
    """A test for the settings page."""
    mock_settings_db = MagicMock()
    mock_get.return_value = mock_settings_db
    mock_projects_db = MagicMock()
    mock_project_get.return_value = mock_projects_db

    settings_df = pd.DataFrame(
        {
            "_id": [1, 2],
            "created_at_": ["2021-01-01", "2021-01-02"],
            "created_at": [
                datetime.fromtimestamp(0, tz=pytz.utc),
                datetime.fromtimestamp(1, tz=pytz.utc),
            ],
            "project_id": ["P1234", "P5678"],
            "name": ["new settings", "another settings"],
            "fasta_file_name": ["fasta_file1", "fasta_file2"],
            "speclib_file_name": ["speclib_file1", "speclib_file2"],
            "config_file_name": ["config_file1", "config_file2"],
            "software": ["software1", "software2"],
            "status": ["active", "inactive"],
        },
    )

    mock_df.return_value = settings_df

    at = AppTest.from_file(f"{APP_FOLDER}/pages/settings.py").run()

    expected_data = {
        "config_file_name": {0: "config_file1", 1: "config_file2"},
        "created_at": {
            0: Timestamp("1970-01-01 00:00:00+0000", tz="UTC"),
            1: Timestamp("1970-01-01 00:00:01+0000", tz="UTC"),
        },
        "created_at_": {0: "2021-01-01", 1: "2021-01-02"},
        "fasta_file_name": {0: "fasta_file1", 1: "fasta_file2"},
        "name": {0: "new settings", 1: "another settings"},
        "project_id": {0: "P1234", 1: "P5678"},
        "software": {0: "software1", 1: "software2"},
        "speclib_file_name": {0: "speclib_file1", 1: "speclib_file2"},
        "status": {0: "active", 1: "inactive"},
    }

    assert not at.exception
    assert expected_data == at.table[0].value.to_dict()


@skip  # TODO: fix this test once the issues with test_add_new_project_form_submission() are fixed
def test_add_new_settings_form_submission() -> None:
    """A test for the form submission on the settings page."""
