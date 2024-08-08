"""Test the status.py file."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from streamlit.testing.v1 import AppTest

APP_FOLDER = Path(__file__).parent / Path("../../")


@patch("service.data_handling.get_combined_raw_files_and_metrics_df")
@patch("service.db.df_from_db_data")
@patch("service.db.get_status_data")
def test_status(
    mock_get_status_data: MagicMock,
    mock_df: MagicMock,
    mock_get: MagicMock,
) -> None:
    """Test that status page renders successfully."""
    # mock_raw_files_db, mock_metrics_db, mock_status_db = (
    #     MagicMock(),
    #     MagicMock(),
    #     MagicMock(),
    # )
    mock_status_db = MagicMock()

    mock_get_status_data.return_value = mock_status_db

    ts1 = pd.to_datetime(datetime.now())  # noqa: DTZ005
    ts2 = pd.to_datetime(datetime.fromtimestamp(5e9 + 0.5))  # noqa: DTZ006
    combined_df = pd.DataFrame(
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
            "status": ["not_done", "not_error"],
            "status_details": ["", ""],
            "instrument_id": ["i1", "i1"],
        },
    )

    mock_get.return_value = combined_df

    status_df = pd.DataFrame(
        {
            "_id": ["i1", "i1"],
            "updated_at_": [
                ts1,
                ts2,
            ],
            "last_error_occurred_at": [
                ts1,
                ts2,
            ],
            "status": ["ok", "error"],
            "status_details": ["", ""],
        }
    )

    mock_df.side_effect = [status_df]

    # when
    at = AppTest.from_file(f"{APP_FOLDER}/pages/status.py").run()

    ts1noms = ts1.replace(microsecond=0)
    result = at.dataframe[0].value.to_dict()

    assert result["instrument_id"] == {0: "i1"}

    assert result["last_file_creation"] == {0: ts1}
    assert result["last_status_update"] == {0: ts2}
    assert result["last_health_check"] == {0: ts1noms}
    assert result["status_details"] == {0: ""}

    for col in [
        "last_health_check_error",
        "last_health_check_text",
        "last_file_creation_text",
        "last_status_update_text",
    ]:
        assert col in result
