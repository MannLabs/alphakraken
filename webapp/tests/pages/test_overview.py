"""Test the app.py file."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from streamlit.testing.v1 import AppTest

APP_FOLDER = Path(__file__).parent / Path("../../")


@patch("service.db.get_all_data")
@patch("service.db.df_from_db_data")
def test_overview(mock_df: MagicMock, mock_get: MagicMock) -> None:
    """A very boring test for a very boring page."""
    mock_raw_files_db, mock_metrics_db = MagicMock(), MagicMock()
    mock_get.return_value = mock_raw_files_db, mock_metrics_db

    raw_files_df = pd.DataFrame(
        {
            "_id": [1, 2],
            "db_entry_created_at": ["2021-01-01", "2021-01-02"],
            "created_at": ["2021-01-01", "2021-01-02"],
        },
    )
    metrics_df = pd.DataFrame({"raw_file": [1, 2], "BasicStats_proteins_mean": [1, 2]})

    mock_df.side_effect = [raw_files_df, metrics_df]

    at = AppTest.from_file(f"{APP_FOLDER}/pages/overview.py").run()

    expected_data = {
        "BasicStats_proteins_mean": {0: 2, 1: 1},
        "_id": {0: 2, 1: 1},
        "created_at": {0: "2021-01-02", 1: "2021-01-01"},
        "db_entry_created_at": {0: "2021-01-02", 1: "2021-01-01"},
        "raw_file": {0: 2, 1: 1},
    }
    assert expected_data == at.dataframe[0].value.to_dict()
