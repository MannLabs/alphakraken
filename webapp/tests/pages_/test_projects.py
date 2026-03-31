"""Tests for the projects page."""

from datetime import datetime
from pathlib import Path
from unittest import skip
from unittest.mock import MagicMock, patch

import pandas as pd
import pytz
from streamlit.testing.v1 import AppTest

PAGES_FOLDER = Path(__file__).parent / Path("../../pages_")


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.ProjectSettings")
@patch("service.db.get_project_data")
@patch("service.db.df_from_db_data")
def test_projects_display_table(
    mock_df: MagicMock,
    mock_get_project: MagicMock,
    mock_project_settings_cls: MagicMock,
    mock_connect_db: MagicMock,  # noqa: ARG001
) -> None:
    """Test that the table shows correctly on the projects page."""
    mock_project1 = MagicMock()
    mock_project1.id = "P1234"
    mock_project2 = MagicMock()
    mock_project2.id = "P5678"

    mock_projects_db = MagicMock()
    mock_projects_db.__iter__ = MagicMock(
        side_effect=lambda: iter([mock_project1, mock_project2])
    )
    mock_get_project.return_value = mock_projects_db

    mock_ps1 = MagicMock()
    mock_ps1.settings.name = "settings1"
    mock_ps1.settings.version = 1

    def ps_objects_side_effect(*args, **kwargs) -> MagicMock:
        mock_qs = MagicMock()
        project_id = kwargs.get("project") or (args[0] if args else None)
        if project_id == "P1234":
            mock_qs.order_by.return_value = [mock_ps1]
        else:
            mock_qs.order_by.return_value = []
        return mock_qs

    mock_project_settings_cls.objects.side_effect = ps_objects_side_effect

    projects_df = pd.DataFrame(
        {
            "_id": ["P1234", "P5678"],
            "created_at_": ["2021-01-01", "2021-01-02"],
            "created_at": [
                datetime.fromtimestamp(0, tz=pytz.utc),
                datetime.fromtimestamp(1, tz=pytz.utc),
            ],
            "name": ["new project", "another project"],
            "description": [
                "some new project description",
                "another project description",
            ],
        },
    )

    mock_df.return_value = projects_df

    at = AppTest.from_file(f"{PAGES_FOLDER}/projects.py").run(timeout=10)

    assert not at.exception
    table_data = at.table[0].value
    assert "settings" in table_data.columns
    assert table_data.loc[0, "settings"] == "settings1 v1"
    assert table_data.loc[1, "settings"] == ""


@skip("")  # TODO: fix this test
@patch("service.db.get_project_data")
@patch("service.db.df_from_db_data")
def test_add_new_project_form_submission(
    mock_df: MagicMock,  # noqa: ARG001
    mock_get: MagicMock,  # noqa: ARG001
) -> None:
    """Test that add_project is called with correct arguments when form is submitted."""
    at = AppTest.from_file(f"{PAGES_FOLDER}/projects.py").run()

    at.text_input[1].set_value("123")
    at.text_input[2].set_value("1234")
    at.button[0].click()
