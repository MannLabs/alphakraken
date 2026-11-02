"""Tests for the projects page."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import pytz
from service.session_state import SessionStateKeys
from streamlit.testing.v1 import AppTest

from shared.db.models import ProjectStatus
from shared.keys import FALLBACK_PROJECT_ID

PAGES_FOLDER = Path(__file__).parent / Path("../../pages_")


@patch("shared.db.interface.connect_db")
@patch("shared.db.interface.ProjectSettings")
@patch("service.db.get_project_data")
@patch("service.db.df_from_db_data")
@patch("service.components.show_filter", side_effect=lambda df, **_kw: (df,))
def test_projects_display_table(
    mock_show_filter: MagicMock,  # noqa: ARG001
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
    mock_ps1.scopes = ["*"]
    mock_ps1.excluded_scopes = []
    mock_ps1.raw_file_id_filter = []
    mock_ps1.raw_file_id_exclude_filter = []

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
    assert table_data.loc[0, "settings"] == "settings1 version 1"
    assert table_data.loc[1, "settings"] == ""


@patch("service.owner.get_all_users")
@patch("service.db.get_project_data")
@patch("service.db.df_from_db_data")
@patch("service.components.show_filter", side_effect=lambda df, **_kw: (df,))
def test_projects_owner_is_shown_and_selectable(
    mock_show_filter: MagicMock,  # noqa: ARG001
    mock_df: MagicMock,
    mock_get_project: MagicMock,
    mock_get_users: MagicMock,
) -> None:
    """Test that owner initials are shown and the active users are offered as owner."""
    mock_get_project.return_value = []
    mock_df.return_value = pd.DataFrame(
        {"_id": ["P1234", "P5678"], "name": ["a", "b"], "owner": ["a@b.de", None]}
    )
    user = MagicMock(initials="AB", email="a@b.de")
    mock_get_users.return_value = [user]

    at = AppTest.from_file(f"{PAGES_FOLDER}/projects.py").run(timeout=10)

    assert not at.exception
    assert at.table[0].value["owner"].tolist() == ["AB", ""]
    owner_select = next(s for s in at.selectbox if s.label == "Owner")
    assert owner_select.options == ["(none)", "AB (a@b.de)"]
    assert owner_select.value is None


ASSIGN_BUTTON_LABEL = "Assign selected settings to project _FALLBACK"


@pytest.mark.parametrize("is_admin", [False, True])
@patch("pages_.impl.projects_utils.get_resolved_settings_df", side_effect=ValueError)
@patch("shared.db.interface.assign_settings_to_project")
@patch("shared.db.interface.get_all_settings")
@patch("shared.db.interface.get_project_settings", return_value=[])
@patch("service.db.get_project_data")
@patch("service.db.df_from_db_data")
@patch("service.components.show_filter", side_effect=lambda df, **_kw: (df,))
def test_fallback_assignment_only_for_admins(  # noqa: PLR0913
    mock_show_filter: MagicMock,  # noqa: ARG001
    mock_df: MagicMock,
    mock_get_project: MagicMock,
    mock_get_project_settings: MagicMock,  # noqa: ARG001
    mock_get_all_settings: MagicMock,
    mock_assign: MagicMock,
    mock_resolved: MagicMock,  # noqa: ARG001
    *,
    is_admin: bool,
) -> None:
    """Test that only admins can assign settings to _FALLBACK."""
    mock_get_project.return_value = [
        MagicMock(id=FALLBACK_PROJECT_ID, status=ProjectStatus.ACTIVE)
    ]
    mock_df.return_value = pd.DataFrame({"_id": [FALLBACK_PROJECT_ID]})
    mock_get_all_settings.return_value = [
        MagicMock(id="S1", version=1, software_type="alphadia", software="sw")
    ]

    at = AppTest.from_file(f"{PAGES_FOLDER}/projects.py")
    if is_admin:
        at.session_state[SessionStateKeys.IS_ADMIN] = True
    at.run(timeout=10)
    at.selectbox(key="assign_project_select").set_value(FALLBACK_PROJECT_ID).run(
        timeout=10
    )

    assert not at.exception
    assign_button = next(b for b in at.button if b.label == ASSIGN_BUTTON_LABEL)
    assert assign_button.disabled != is_admin

    if is_admin:
        assign_button.click().run(timeout=10)

        assert not at.exception
        mock_assign.assert_called_once()


@patch("shared.db.interface.add_project")
@patch("service.owner.get_all_users")
@patch("service.db.get_project_data", return_value=[])
@patch("service.db.df_from_db_data", return_value=pd.DataFrame())
@patch("service.components.show_filter", side_effect=lambda df, **_kw: (df,))
def test_add_project_passes_selected_owner(
    mock_show_filter: MagicMock,  # noqa: ARG001
    mock_df: MagicMock,  # noqa: ARG001
    mock_get_project: MagicMock,  # noqa: ARG001
    mock_get_users: MagicMock,
    mock_add_project: MagicMock,
) -> None:
    """Test that the owner selected in the form is passed on when creating a project."""
    mock_get_users.return_value = [MagicMock(initials="AB", email="a@b.de")]
    at = AppTest.from_file(f"{PAGES_FOLDER}/projects.py").run(timeout=10)

    next(t for t in at.text_input if t.label.startswith("Project Name")).set_value(
        "name"
    )
    next(t for t in at.text_input if t.label.startswith("Project Id")).set_value(
        "ABCD1"
    )
    next(s for s in at.selectbox if s.label == "Owner").set_value("a@b.de")
    next(b for b in at.button if b.label == "Create project").click().run(timeout=10)

    assert not at.exception
    mock_add_project.assert_called_once_with(
        project_id="ABCD1", name="name", description="", owner_email="a@b.de"
    )
