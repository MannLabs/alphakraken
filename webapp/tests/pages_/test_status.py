"""Test the status.py file."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from streamlit.testing.v1 import AppTest

PAGES_FOLDER = Path(__file__).parent / Path("../../pages_")


@patch("service.db.get_raw_files_for_status_df")
@patch("service.db.df_from_db_data")
@patch("service.db.get_status_data")
def test_status(
    mock_get_status_data: MagicMock,
    mock_df_from_db_data: MagicMock,
    mock_get_raw_files_for_status_df: MagicMock,
) -> None:
    """Test that status page renders successfully."""
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
            "status": ["not_done", "not_error"],
            "status_details": ["", ""],
            "instrument_id": ["i1", "i1"],
        },
    )

    mock_get_raw_files_for_status_df.return_value = combined_df

    status_df = pd.DataFrame(
        {
            "_id": ["i1", "i1"],
            "updated_at_": [
                ts1,
                ts2,
            ],
            "status": ["ok", "error"],
            "status_details": ["", ""],
            "free_space_gb": [100, 200],
        }
    )

    mock_df_from_db_data.side_effect = [status_df]

    # when
    at = AppTest.from_file(f"{PAGES_FOLDER}/status.py").run()

    ts1noms = ts1.replace(microsecond=0)
    assert not at.exception

    result = at.dataframe[0].value.to_dict()
    assert result["instrument_id"] == {0: "i1"}

    assert result["last_file_creation"] == {0: ts1}
    assert result["last_status_update"] == {0: ts2}
    assert result["last_health_check"] == {0: ts1noms}
    assert result["status_details"] == {0: ""}

    for col in [
        "last_health_check_text",
        "last_file_creation_text",
        "last_status_update_text",
    ]:
        assert col in result
