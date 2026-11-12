"""Tests for the projects page."""

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from service.session_state import SessionStateKeys
from streamlit.testing.v1 import AppTest

from shared.domain.settings import SettingsStatus
from shared.keys import FALLBACK_PROJECT_ID
from shared.tests.factories import project, project_settings, settings, user

PAGES_FOLDER = Path(__file__).parent / Path("../../pages_")

YOUNGER = datetime(2021, 1, 2, tzinfo=UTC)
OLDER = datetime(2021, 1, 1, tzinfo=UTC)


@pytest.fixture(autouse=True)
def mock_find_assignments() -> Iterator[None]:
    """No settings are assigned unless a test says otherwise."""
    with patch("shared.db.project_settings.find", return_value=[]) as mock_find:
        yield mock_find


@patch("shared.db.projects.find")
@patch("service.components.show_filter", side_effect=lambda df, **_kw: (df,))
def test_projects_display_table(
    mock_show_filter: MagicMock,  # noqa: ARG001
    mock_find_projects: MagicMock,
    mock_find_assignments: MagicMock,
) -> None:
    """Test that the table shows the projects, youngest first, with their assigned settings."""
    mock_find_projects.return_value = [
        project(id="P1234", name="new project", created_at_=YOUNGER),
        project(id="P5678", name="another project", created_at_=OLDER),
    ]
    mock_find_assignments.return_value = [
        project_settings(
            project_id="P1234", settings=settings(name="settings1", version=1)
        )
    ]

    at = AppTest.from_file(f"{PAGES_FOLDER}/projects.py").run(timeout=10)

    assert not at.exception
    mock_find_assignments.assert_called_once_with(["P1234", "P5678"])
    table_data = at.table[0].value
    assert "settings" in table_data.columns
    assert table_data.loc[0, "settings"] == "settings1 version 1"
    assert table_data.loc[1, "settings"] == ""


@patch("shared.db.users.find")
@patch("shared.db.projects.find")
@patch("service.components.show_filter", side_effect=lambda df, **_kw: (df,))
def test_projects_owner_is_shown_and_selectable(
    mock_show_filter: MagicMock,  # noqa: ARG001
    mock_find_projects: MagicMock,
    mock_get_users: MagicMock,
) -> None:
    """Test that owner initials are shown and the active users are offered as owner."""
    mock_find_projects.return_value = [
        project(id="P1234", owner_email="a@b.de", created_at_=YOUNGER),
        project(id="P5678", owner_email=None, created_at_=OLDER),
    ]
    mock_get_users.return_value = [user(initials="AB", email="a@b.de")]

    at = AppTest.from_file(f"{PAGES_FOLDER}/projects.py").run(timeout=10)

    assert not at.exception
    assert at.table[0].value["owner"].tolist() == ["AB", ""]
    owner_select = next(s for s in at.selectbox if s.label == "Owner")
    assert owner_select.options == ["(none)", "AB (a@b.de)"]
    assert owner_select.value is None


ASSIGN_BUTTON_LABEL = "Assign selected settings to project _FALLBACK"


@pytest.mark.parametrize("is_admin", [False, True])
@patch("pages_.impl.projects_utils.get_resolved_settings_df", side_effect=ValueError)
@patch("shared.db.project_settings.assign")
@patch("shared.db.settings.find")
@patch("shared.db.projects.find")
@patch("service.components.show_filter", side_effect=lambda df, **_kw: (df,))
def test_fallback_assignment_only_for_admins(  # noqa: PLR0913
    mock_show_filter: MagicMock,  # noqa: ARG001
    mock_find_projects: MagicMock,
    mock_find_settings: MagicMock,
    mock_assign: MagicMock,
    mock_resolved: MagicMock,  # noqa: ARG001
    *,
    is_admin: bool,
) -> None:
    """Test that only admins can assign settings to _FALLBACK."""
    mock_find_projects.return_value = [project(id=FALLBACK_PROJECT_ID)]
    mock_find_settings.return_value = [
        settings(),
        # archived settings are not offered
        settings(id="old_v1", name="old", version=1, status=SettingsStatus.INACTIVE),
    ]

    at = AppTest.from_file(f"{PAGES_FOLDER}/projects.py")
    if is_admin:
        at.session_state[SessionStateKeys.IS_ADMIN] = True
    at.run(timeout=10)
    at.selectbox(key="assign_project_select").set_value(FALLBACK_PROJECT_ID).run(
        timeout=10
    )

    assert not at.exception
    assert len(at.selectbox(key="assign_settings_select").options) == 1
    assign_button = next(b for b in at.button if b.label == ASSIGN_BUTTON_LABEL)
    assert assign_button.disabled != is_admin

    if is_admin:
        assign_button.click().run(timeout=10)

        assert not at.exception
        mock_assign.assert_called_once()
        # the settings are identified by their unique name
        assert mock_assign.call_args.args == (FALLBACK_PROJECT_ID, "fallback_v7")


@patch("shared.db.projects.add")
@patch("shared.db.users.find")
@patch("shared.db.projects.find", return_value=[])
@patch("service.components.show_filter", side_effect=lambda df, **_kw: (df,))
def test_add_project_passes_selected_owner(
    mock_show_filter: MagicMock,  # noqa: ARG001
    mock_find_projects: MagicMock,  # noqa: ARG001
    mock_get_users: MagicMock,
    mock_add_project: MagicMock,
) -> None:
    """Test that the owner selected in the form is passed on when creating a project."""
    mock_get_users.return_value = [user(initials="AB", email="a@b.de")]
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
