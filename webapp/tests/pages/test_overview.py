"""Test the overview.py file."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from streamlit.testing.v1 import AppTest

APP_FOLDER = Path(__file__).parent / Path("../../")


@patch("service.db.get_raw_file_and_metrics_data")
@patch("service.db.df_from_db_data")
@patch("service.db.get_status_data")
def test_overview(
    mock_get_status_data: MagicMock,  # noqa: ARG001
    mock_df_from_db_data: MagicMock,
    mock_get_raw_file_and_metrics_data: MagicMock,
) -> None:
    """Test that overview page renders successfully."""
    mock_raw_files_db, mock_metrics_db, mock_status_db = (
        MagicMock(),
        MagicMock(),
        MagicMock(),
    )
    mock_get_raw_file_and_metrics_data.side_effect = [
        (mock_raw_files_db, mock_metrics_db),
        mock_status_db,
    ]

    ts1 = pd.to_datetime(datetime.now())  # noqa: DTZ005
    ts2 = pd.to_datetime(datetime.fromtimestamp(5e9 + 0.5))  # noqa: DTZ006
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
            "project_id": ["P1", "P2"],
            "status": ["done", "error"],
            "status_details": ["", ""],
            "instrument_id": ["i1", "i1"],
        },
    )
    metrics_df = pd.DataFrame(
        {
            "raw_file": [1, 2],
            "proteins": [1, 2],
            "precursors": [1, 2],
            "ms1_accuracy": [1.0, 2.0],
            "fwhm_rt": [1.0, 2.0],
            "quanting_time_elapsed": [60.0, 120.0],
            "weighted_ms1_intensity_sum": [1.0, 2.0],
            "intensity_sum": [1.0, 2.0],
            "settings_version": [1, 2],
        }
    )

    custom_metrics_df = pd.DataFrame(
        {
            "raw_file": [1, 2],
            "proteins": [1, 2],
            "some_custom_metric": [1, 2],
        }
    )

    status_df = pd.DataFrame(
        {
            "_id": ["i1", "i1"],
            "updated_at_": [
                ts1,
                ts2,
            ],
            "status": ["ok", "error"],
            "status_details": ["", ""],
        }
    )

    mock_df_from_db_data.side_effect = [
        raw_files_df,
        metrics_df,
        custom_metrics_df,
        status_df,
    ]

    # when
    at = AppTest.from_file(f"{APP_FOLDER}/pages/overview.py").run()

    ts1str = ts1.strftime("%Y-%m-%d %H:%M:%S")
    ts2str = ts2.strftime("%Y-%m-%d %H:%M:%S")
    ts1noms = ts1.replace(microsecond=0)
    ts2noms = ts2.replace(microsecond=0)

    expected_data = {
        "_id": {1: 1, 2: 2},
        "size": {1: 1073741824, 2: 2147483648},
        "quanting_time_elapsed": {1: 60.0, 2: 120.0},
        "raw_file": {1: 1, 2: 2},
        "proteins": {1: 1, 2: 2},
        "precursors": {1: 1, 2: 2},
        "ms1_median_accuracy": {1: 1.0, 2: 2.0},
        "fwhm_rt": {1: 1.0, 2: 2.0},
        "created_at": {1: ts1noms, 2: ts2noms},
        "created_at_": {1: ts1noms, 2: ts2noms},
        "updated_at_": {1: ts1noms, 2: ts2noms},
        "file_created": {1: ts1str, 2: ts2str},
        "quanting_time_minutes": {1: 1.0, 2: 2.0},
        "size_gb": {1: 1.0, 2: 2.0},
        "project_id": {1: "P1", 2: "P2"},
        "status": {1: "done", 2: "error"},
        "status_details": {1: "", 2: ""},
        "instrument_id": {1: "i1", 2: "i1"},
        "weighted_ms1_intensity_sum": {1: 1.0, 2: 2.0},
        "intensity_sum": {1: 1.0, 2: 2.0},
        "settings_version": {1: 1, 2: 2},
        "proteins_custom": {1: 1, 2: 2},
        "some_custom_metric": {1: 1, 2: 2},
    }

    assert not at.exception
    assert at.dataframe[0].value.to_dict() == expected_data
    # plots not tested
