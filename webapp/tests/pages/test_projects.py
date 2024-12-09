"""Tests for the projects page."""

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
@patch("service.db.df_from_db_data")
def test_projects_display_table(mock_df: MagicMock, mock_get: MagicMock) -> None:
    """Test that the table shows correctly on the projects page."""
    mock_projects_db = MagicMock()
    mock_get.return_value = mock_projects_db

    projects_df = pd.DataFrame(
        {
            "_id": [1, 2],
            "created_at_": ["2021-01-01", "2021-01-02"],
            "created_at": [
                datetime.fromtimestamp(0, tz=pytz.utc),
                datetime.fromtimestamp(1, tz=pytz.utc),
            ],
            "project_id": ["P1234", "P5678"],
            "name": ["new project", "another project"],
            "description": [
                "some new project description",
                "another project description",
            ],
        },
    )

    mock_df.return_value = projects_df

    at = AppTest.from_file(f"{APP_FOLDER}/pages/projects.py").run()

    expected_data = {
        "_id": {0: 1, 1: 2},
        "created_at": {
            0: Timestamp("1970-01-01 00:00:00+0000", tz="UTC"),
            1: Timestamp("1970-01-01 00:00:01+0000", tz="UTC"),
        },
        "created_at_": {0: "2021-01-01", 1: "2021-01-02"},
        "description": {
            0: "some new project description",
            1: "another project description",
        },
        "name": {0: "new project", 1: "another project"},
        "project_id": {0: "P1234", 1: "P5678"},
    }

    assert not at.exception
    assert expected_data == at.table[0].value.to_dict()


@skip  # TODO: fix this test
@patch("service.db.get_project_data")
@patch("service.db.df_from_db_data")
def test_add_new_project_form_submission(
    mock_df: MagicMock,  # noqa: ARG001
    mock_get: MagicMock,  # noqa: ARG001
) -> None:
    """Test that add_project is called with correct arguments when form is submitted."""
    # given
    # project_id = "P1234"
    # name = "new project"
    # description = "some new project description"

    # when
    # Problem 1: this patching seems to interfere with the form
    #    with patch("pages.projects.add_project") as mock_add_project:
    at = AppTest.from_file(f"{APP_FOLDER}/pages/projects.py").run()

    # Problem 2: this does not do anything on the form:
    at.text_input[1].set_value("123")
    at.text_input[2].set_value("1234")
    at.button[0].click()
