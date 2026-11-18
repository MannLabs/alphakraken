"""Tests for the settings page."""

from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import pytz
from pandas import Timestamp
from streamlit.testing.v1 import AppTest

from shared.domain.errors import ValidationError
from shared.runners import RUNNERS

PAGES_FOLDER = Path(__file__).parent / Path("../../pages_")

RUNNER_SELECT_LABEL = "Runner"
SOFTWARE_SELECT_LABEL = "Software*"
ADD_NEW_SOFTWARE_OPTION = "➕ Add new software..."  # noqa: RUF001
NEW_SOFTWARE_INPUT_LABEL = "New software"


def _settings_df() -> pd.DataFrame:
    """Get two active settings entries as the page reads them from the DB."""
    return pd.DataFrame(
        {
            "id": ["new settings_v1", "another settings_v1"],
            "created_at_": ["2021-01-01", "2021-01-02"],
            "created_at": [
                datetime.fromtimestamp(0, tz=pytz.utc),
                datetime.fromtimestamp(1, tz=pytz.utc),
            ],
            "project_id": ["P1234", "P5678"],
            "name": ["new settings", "another settings"],
            "version": [1, 1],
            "fasta_file_name": ["fasta_file1", "fasta_file2"],
            "speclib_file_name": ["speclib_file1", "speclib_file2"],
            "config_file_name": ["config_file1", "config_file2"],
            "software": ["software1", "software2"],
            "status": ["active", "active"],
        },
    )


@patch("shared.db.project_settings.find", return_value=[])
@patch("shared.db.projects.find", return_value=[])
@patch("shared.db.settings.find", return_value=[])
@patch("service.db.df_from_db_data")
def test_settings(
    mock_df: MagicMock,
    mock_get: MagicMock,  # noqa: ARG001
    mock_project_get: MagicMock,  # noqa: ARG001
    mock_ps_objects: MagicMock,  # noqa: ARG001
) -> None:
    """A test for the settings page."""
    mock_df.return_value = _settings_df()

    at = AppTest.from_file(f"{PAGES_FOLDER}/settings.py").run(timeout=10)

    expected_data = {
        "config_file_name": {0: "config_file1", 1: "config_file2"},
        "created_at": {
            0: Timestamp("1970-01-01 00:00:00+0000", tz="UTC"),
            1: Timestamp("1970-01-01 00:00:01+0000", tz="UTC"),
        },
        "created_at_": {0: "2021-01-01", 1: "2021-01-02"},
        "fasta_file_name": {0: "fasta_file1", 1: "fasta_file2"},
        "name": {0: "new settings", 1: "another settings"},
        "project_id": {0: "P1234", 1: "P5678"},
        "software": {0: "software1", 1: "software2"},
        "speclib_file_name": {0: "speclib_file1", 1: "speclib_file2"},
        "status": {0: "active", 1: "active"},
        "version": {0: 1, 1: 1},
    }

    assert not at.exception
    assert expected_data == at.table[0].value.to_dict()


@pytest.mark.parametrize("software_type", ["msqc", "skyline", "custom"])
@patch("shared.db.project_settings.find", return_value=[])
@patch("shared.db.projects.find", return_value=[])
@patch("shared.db.settings.find", return_value=[])
@patch("service.db.df_from_db_data")
def test_settings_renders_for_software_types_without_file_inputs(
    mock_df: MagicMock,
    mock_get: MagicMock,  # noqa: ARG001
    mock_project_get: MagicMock,  # noqa: ARG001
    mock_ps_objects: MagicMock,  # noqa: ARG001
    software_type: str,
) -> None:
    """A software type that lacks some of the file name inputs does not break the page."""
    mock_df.return_value = _settings_df()
    at = AppTest.from_file(f"{PAGES_FOLDER}/settings.py").run(timeout=10)

    next(s for s in at.selectbox if s.label == "Type").set_value(software_type).run(
        timeout=10
    )

    assert not at.exception


@patch("shared.db.project_settings.find", return_value=[])
@patch("shared.db.projects.find", return_value=[])
@patch("shared.db.settings.find", return_value=[])
@patch("service.db.df_from_db_data")
def test_settings_runner_selectbox_offers_the_declared_runners(
    mock_df: MagicMock,
    mock_get: MagicMock,  # noqa: ARG001
    mock_project_get: MagicMock,  # noqa: ARG001
    mock_ps_objects: MagicMock,  # noqa: ARG001
) -> None:
    """Test that the runner selectbox lists the runners of alphakraken.yaml in declaration order."""
    mock_df.return_value = _settings_df()

    at = AppTest.from_file(f"{PAGES_FOLDER}/settings.py").run(timeout=10)

    assert not at.exception
    runner_selects = [s for s in at.selectbox if s.label == RUNNER_SELECT_LABEL]
    assert len(runner_selects) == 1
    assert runner_selects[0].options == list(RUNNERS)
    assert runner_selects[0].value == next(iter(RUNNERS))


@patch.dict(RUNNERS, {}, clear=True)
@patch("shared.db.project_settings.find", return_value=[])
@patch("shared.db.projects.find", return_value=[])
@patch("shared.db.settings.find", return_value=[])
@patch("service.db.df_from_db_data")
def test_settings_without_runners_shows_notice_instead_of_form(
    mock_df: MagicMock,
    mock_get: MagicMock,  # noqa: ARG001
    mock_project_get: MagicMock,  # noqa: ARG001
    mock_ps_objects: MagicMock,  # noqa: ARG001
) -> None:
    """Test that a deployment without runners still lists settings but cannot create any."""
    mock_df.return_value = _settings_df()

    at = AppTest.from_file(f"{PAGES_FOLDER}/settings.py").run(timeout=10)

    assert not at.exception
    assert len(at.table) == 1
    assert any("runners" in w.value for w in at.warning)
    assert [s for s in at.selectbox if s.label == RUNNER_SELECT_LABEL] == []


def _settings_df_with_software() -> pd.DataFrame:
    """Get settings entries of mixed software type and runner, youngest first, as df_from_db_data sorts them."""
    return pd.DataFrame(
        {
            "id": [
                "young_v2",
                "old_v1",
                "other_type_v1",
                "other_runner_v1",
                "retired_runner_v1",
            ],
            "created_at_": [
                "2021-01-05",
                "2021-01-04",
                "2021-01-03",
                "2021-01-02",
                "2021-01-01",
            ],
            "name": ["young", "old", "other_type", "other_runner", "retired_runner"],
            "version": [2, 1, 1, 1, 1],
            "software": [
                "alphadia-2.0.0",
                "alphadia-1.10.0",
                "msqc/run_msqc.sh",
                "alphadia-on-docker",
                "alphadia-retired",
            ],
            "software_type": ["alphadia", "alphadia", "msqc", "alphadia", "alphadia"],
            "runner_name": ["slurm", "slurm", "slurm", "docker", "gone_from_the_yaml"],
            "status": ["active", "inactive", "active", "active", "active"],
        },
    )


@patch("shared.db.project_settings.find", return_value=[])
@patch("shared.db.projects.find", return_value=[])
@patch("shared.db.settings.find", return_value=[])
@patch("service.db.df_from_db_data")
def test_settings_software_selectbox_offers_the_used_software(
    mock_df: MagicMock,
    mock_get: MagicMock,  # noqa: ARG001
    mock_project_get: MagicMock,  # noqa: ARG001
    mock_ps_objects: MagicMock,  # noqa: ARG001
) -> None:
    """Test that the software of the selected type and runner is offered, youngest first, archived included."""
    mock_df.return_value = _settings_df_with_software()

    at = AppTest.from_file(f"{PAGES_FOLDER}/settings.py").run(timeout=10)

    assert not at.exception
    software_selects = [s for s in at.selectbox if s.label == SOFTWARE_SELECT_LABEL]
    assert len(software_selects) == 1
    # 'msqc/run_msqc.sh' is another software type, 'alphadia-on-docker' another runner
    assert software_selects[0].options == [
        "alphadia-2.0.0",
        "alphadia-1.10.0",
        ADD_NEW_SOFTWARE_OPTION,
    ]
    assert any(
        "`alphadia-2.0.0` is already used by `young` v2" in m.value for m in at.markdown
    )


@patch("shared.db.project_settings.find", return_value=[])
@patch("shared.db.projects.find", return_value=[])
@patch("shared.db.settings.find", return_value=[])
@patch("service.db.df_from_db_data")
def test_settings_software_selectbox_without_used_software_offers_adding_one(
    mock_df: MagicMock,
    mock_get: MagicMock,  # noqa: ARG001
    mock_project_get: MagicMock,  # noqa: ARG001
    mock_ps_objects: MagicMock,  # noqa: ARG001
) -> None:
    """Test that a deployment without matching settings falls back to entering the software by hand."""
    mock_df.return_value = _settings_df()  # has no software_type and runner_name

    at = AppTest.from_file(f"{PAGES_FOLDER}/settings.py").run(timeout=10)

    assert not at.exception
    software_selects = [s for s in at.selectbox if s.label == SOFTWARE_SELECT_LABEL]
    assert software_selects[0].options == [ADD_NEW_SOFTWARE_OPTION]
    assert [t for t in at.text_input if t.label == NEW_SOFTWARE_INPUT_LABEL]
    assert any(
        "administrator needs to make it available" in w.value for w in at.warning
    )


@patch("shared.db.project_settings.find", return_value=[])
@patch("shared.db.projects.find", return_value=[])
@patch("shared.db.settings.find", return_value=[])
@patch("service.db.df_from_db_data")
def test_settings_software_selectbox_keeps_a_software_the_runner_never_ran(
    mock_df: MagicMock,
    mock_get: MagicMock,  # noqa: ARG001
    mock_project_get: MagicMock,  # noqa: ARG001
    mock_ps_objects: MagicMock,  # noqa: ARG001
) -> None:
    """Test that updating settings whose software is not in the list selects it rather than 'add new'."""
    mock_df.return_value = _settings_df_with_software()

    at = AppTest.from_file(f"{PAGES_FOLDER}/settings.py").run(timeout=10)
    # the runner of 'retired_runner' is gone, so the page falls back to 'slurm', which never ran its software
    at.selectbox[0].select("retired_runner").run(timeout=10)

    assert not at.exception
    software_select = next(s for s in at.selectbox if s.label == SOFTWARE_SELECT_LABEL)
    assert software_select.value == "alphadia-retired"
    assert software_select.options[-1] == ADD_NEW_SOFTWARE_OPTION
    assert [t for t in at.text_input if t.label == NEW_SOFTWARE_INPUT_LABEL] == []
    assert not any(
        "administrator needs to make it available" in w.value for w in at.warning
    )


OWNER_SELECT_LABEL = "Owner"


@patch("shared.db.users.find")
@patch("shared.db.project_settings.find", return_value=[])
@patch("shared.db.projects.find", return_value=[])
@patch("shared.db.settings.find", return_value=[])
@patch("service.db.df_from_db_data")
def test_settings_owner_is_shown_and_prefilled_for_new_version(
    mock_df: MagicMock,
    mock_get: MagicMock,  # noqa: ARG001
    mock_project_get: MagicMock,  # noqa: ARG001
    mock_ps_objects: MagicMock,  # noqa: ARG001
    mock_get_users: MagicMock,
) -> None:
    """Test that the owner initials are shown and a new version is prefilled with the previous owner."""
    mock_df.return_value = _settings_df().assign(owner=["a@b.de", None])
    user = MagicMock(initials="AB", email="a@b.de")
    mock_get_users.return_value = [user]

    at = AppTest.from_file(f"{PAGES_FOLDER}/settings.py").run(timeout=10)

    assert not at.exception
    assert at.table[0].value["owner"].tolist() == ["AB", ""]
    owner_select = next(s for s in at.selectbox if s.label == OWNER_SELECT_LABEL)
    assert owner_select.options == ["(none)", "AB (a@b.de)"]
    assert owner_select.value is None

    at.selectbox[0].set_value("new settings").run(timeout=10)

    assert not at.exception
    owner_select = next(s for s in at.selectbox if s.label == OWNER_SELECT_LABEL)
    assert owner_select.value == "a@b.de"


@patch("shared.db.project_settings.find", return_value=[])
@patch("shared.db.projects.find", return_value=[])
@patch("shared.db.settings.find", return_value=[])
@patch("service.db.df_from_db_data")
def test_settings_unset_fields_are_prefilled_empty_for_new_version(
    mock_df: MagicMock,
    mock_get: MagicMock,  # noqa: ARG001
    mock_project_get: MagicMock,  # noqa: ARG001
    mock_ps_objects: MagicMock,  # noqa: ARG001
) -> None:
    """Test that fields the previous version left unset are prefilled empty, not with "None"."""
    mock_df.return_value = _settings_df().assign(
        description=[None, None],
        speclib_file_name=[None, "speclib_file2"],
        config_params=[None, None],
    )

    at = AppTest.from_file(f"{PAGES_FOLDER}/settings.py").run(timeout=10)
    at.selectbox[0].set_value("new settings").run(timeout=10)

    assert not at.exception
    prefilled = [w.value for w in [*at.text_input, *at.text_area]]
    assert "fasta_file1" in prefilled
    assert not {"None", "nan"} & set(prefilled)


@patch("shared.db.project_settings.find", return_value=[])
@patch("shared.db.projects.find", return_value=[])
@patch("shared.db.settings.find", return_value=[])
@patch("service.db.df_from_db_data")
def test_settings_without_users_shows_no_owner_select(
    mock_df: MagicMock,
    mock_get: MagicMock,  # noqa: ARG001
    mock_project_get: MagicMock,  # noqa: ARG001
    mock_ps_objects: MagicMock,  # noqa: ARG001
) -> None:
    """Test that there is no owner selectbox without users."""
    mock_df.return_value = _settings_df()

    at = AppTest.from_file(f"{PAGES_FOLDER}/settings.py").run(timeout=10)

    assert not at.exception
    assert OWNER_SELECT_LABEL not in [s.label for s in at.selectbox]


@patch("shared.db.settings.create")
@patch("shared.db.users.find")
@patch("shared.db.project_settings.find", return_value=[])
@patch("shared.db.projects.find", return_value=[])
@patch("shared.db.settings.find", return_value=[])
@patch("service.db.df_from_db_data")
def test_create_settings_passes_selected_owner(  # noqa: PLR0913
    mock_df: MagicMock,
    mock_get: MagicMock,  # noqa: ARG001
    mock_project_get: MagicMock,  # noqa: ARG001
    mock_ps_objects: MagicMock,  # noqa: ARG001
    mock_get_users: MagicMock,
    mock_create_settings: MagicMock,
) -> None:
    """Test that the owner selected in the form is passed on when creating settings."""
    mock_df.return_value = _settings_df()
    mock_get_users.return_value = [MagicMock(initials="AB", email="a@b.de")]
    at = AppTest.from_file(f"{PAGES_FOLDER}/settings.py").run(timeout=30)
    at.selectbox[0].set_value("new settings").run(timeout=10)

    next(s for s in at.selectbox if s.label == OWNER_SELECT_LABEL).set_value("a@b.de")
    next(c for c in at.checkbox if c.label.startswith("I have uploaded")).check()
    next(b for b in at.button if b.label.startswith("Update settings")).click().run(
        timeout=10
    )

    assert not at.exception
    assert mock_create_settings.call_args.kwargs["owner_email"] == "a@b.de"


@patch("shared.db.settings.create", side_effect=ValidationError(["archiving failed"]))
@patch("shared.db.users.find", return_value=[])
@patch("shared.db.project_settings.find", return_value=[])
@patch("shared.db.projects.find", return_value=[])
@patch("shared.db.settings.find", return_value=[])
@patch("service.db.df_from_db_data")
def test_create_settings_reloads_settings_after_error(  # noqa: PLR0913
    mock_df: MagicMock,
    mock_find_settings: MagicMock,
    mock_project_get: MagicMock,  # noqa: ARG001
    mock_ps_objects: MagicMock,  # noqa: ARG001
    mock_get_users: MagicMock,  # noqa: ARG001
    mock_create_settings: MagicMock,
) -> None:
    """Test that the settings are read anew after a failed create: they exist if only archiving the previous version failed."""
    mock_df.return_value = _settings_df()
    at = AppTest.from_file(f"{PAGES_FOLDER}/settings.py").run(timeout=30)
    at.selectbox[0].set_value("new settings").run(timeout=10)
    next(c for c in at.checkbox if c.label.startswith("I have uploaded")).check()
    reads_before = mock_find_settings.call_count

    next(b for b in at.button if b.label.startswith("Update settings")).click().run(
        timeout=10
    )

    assert not at.exception
    mock_create_settings.assert_called_once()
    assert mock_find_settings.call_count == reads_before + 1
