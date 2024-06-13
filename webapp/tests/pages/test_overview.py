"""Test the overview.py file."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytz
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
            "created_at": [
                datetime.fromtimestamp(0, tz=pytz.utc),
                datetime.fromtimestamp(1, tz=pytz.utc),
            ],
            "size": [1024**3, 2 * 1024**3],
        },
    )
    metrics_df = pd.DataFrame(
        {
            "raw_file": [1, 2],
            "BasicStats_proteins_mean": [1, 2],
            "time_elapsed": [60, 120],
        }
    )

    mock_df.side_effect = [raw_files_df, metrics_df]

    at = AppTest.from_file(f"{APP_FOLDER}/pages/overview.py").run()

    expected_data = {
        "BasicStats_proteins_mean": {1: 1, 2: 2},
        "db_entry_created_at": {1: "2021-01-01", 2: "2021-01-02"},
        "file_created": {1: "1970-01-01 01:00:00", 2: "1970-01-01 01:00:01"},
        "quanting_time_minutes": {1: 1.0, 2: 2.0},
        "size_gb": {1: 1.0, 2: 2.0},
    }

    assert expected_data == at.dataframe[0].value.to_dict()
