"""Test the overview.py file."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from streamlit.testing.v1 import AppTest

APP_FOLDER = Path(__file__).parent / Path("../../")


@patch("service.db.get_raw_file_and_metrics_data")
@patch("service.db.df_from_db_data")
def test_overview(mock_df: MagicMock, mock_get: MagicMock) -> None:
    """A very boring test for a very boring page."""
    mock_raw_files_db, mock_metrics_db = MagicMock(), MagicMock()
    mock_get.return_value = mock_raw_files_db, mock_metrics_db

    ts1 = pd.to_datetime(datetime.now())  # noqa: DTZ005
    ts2 = pd.to_datetime(datetime.fromtimestamp(int(5e9)))  # noqa: DTZ006
    raw_files_df = pd.DataFrame(
        {
            "_id": [1, 2],
            "created_at_": [
                ts1,
                ts2,
            ],
            "updated_at_": [
                ts1,
                ts2,
            ],
            "created_at": [
                ts1,
                ts2,
            ],
            "size": [1024**3, 2 * 1024**3],
        },
    )
    metrics_df = pd.DataFrame(
        {
            "raw_file": [1, 2],
            "proteins_mean": [1, 2],
            "quanting_time_elapsed": [60, 120],
        }
    )

    mock_df.side_effect = [raw_files_df, metrics_df]

    at = AppTest.from_file(f"{APP_FOLDER}/pages/overview.py").run()

    ts1str = ts1.strftime("%Y-%m-%d %H:%M:%S")
    ts2str = ts2.strftime("%Y-%m-%d %H:%M:%S")

    expected_data = {
        "proteins_mean": {1: 1, 2: 2},
        "created_at": {1: ts1, 2: ts2},
        "created_at_": {1: ts1, 2: ts2},
        "updated_at_": {1: ts1, 2: ts2},
        "file_created": {1: ts1str, 2: ts2str},
        "quanting_time_minutes": {1: 1.0, 2: 2.0},
        "size_gb": {1: 1.0, 2: 2.0},
    }

    assert at.dataframe[0].value.to_dict() == expected_data
