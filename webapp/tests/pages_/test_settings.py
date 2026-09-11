"""Tests for the settings page."""

from datetime import datetime
from pathlib import Path
from unittest import skip
from unittest.mock import MagicMock, patch

import pandas as pd
import pytz
from pandas import Timestamp
from streamlit.testing.v1 import AppTest

from shared.runners import RUNNERS

PAGES_FOLDER = Path(__file__).parent / Path("../../pages_")

RUNNER_SELECT_LABEL = "Runner"
SOFTWARE_SELECT_LABEL = "Software*"
ADD_NEW_SOFTWARE_OPTION = "➕ Add new software..."  # noqa: RUF001


def _settings_df() -> pd.DataFrame:
    """Get two active settings entries as the page reads them from the DB."""
    return pd.DataFrame(
        {
            "_id": [1, 2],
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


@patch("shared.db.models.ProjectSettings.objects")
@patch("service.db.get_project_data")
@patch("service.db.get_settings_data")
@patch("service.db.df_from_db_data")
def test_settings(
    mock_df: MagicMock,
    mock_get: MagicMock,
    mock_project_get: MagicMock,
    mock_ps_objects: MagicMock,
) -> None:
    """A test for the settings page."""
    mock_settings_db = MagicMock()
    mock_get.return_value = mock_settings_db
    mock_projects_db = MagicMock()
    mock_project_get.return_value = mock_projects_db
    mock_ps_objects.all.return_value = []
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


@patch("shared.db.models.ProjectSettings.objects")
@patch("service.db.get_project_data")
@patch("service.db.get_settings_data")
@patch("service.db.df_from_db_data")
def test_settings_runner_selectbox_offers_the_declared_runners(
    mock_df: MagicMock,
    mock_get: MagicMock,  # noqa: ARG001
    mock_project_get: MagicMock,  # noqa: ARG001
    mock_ps_objects: MagicMock,
) -> None:
    """Test that the runner selectbox lists the runners of alphakraken.yaml in declaration order."""
    mock_ps_objects.all.return_value = []
    mock_df.return_value = _settings_df()

    at = AppTest.from_file(f"{PAGES_FOLDER}/settings.py").run(timeout=10)

    assert not at.exception
    runner_selects = [s for s in at.selectbox if s.label == RUNNER_SELECT_LABEL]
    assert len(runner_selects) == 1
    assert runner_selects[0].options == list(RUNNERS)
    assert runner_selects[0].value == next(iter(RUNNERS))


@patch.dict(RUNNERS, {}, clear=True)
@patch("shared.db.models.ProjectSettings.objects")
@patch("service.db.get_project_data")
@patch("service.db.get_settings_data")
@patch("service.db.df_from_db_data")
def test_settings_without_runners_shows_notice_instead_of_form(
    mock_df: MagicMock,
    mock_get: MagicMock,  # noqa: ARG001
    mock_project_get: MagicMock,  # noqa: ARG001
    mock_ps_objects: MagicMock,
) -> None:
    """Test that a deployment without runners still lists settings but cannot create any."""
    mock_ps_objects.all.return_value = []
    mock_df.return_value = _settings_df()

    at = AppTest.from_file(f"{PAGES_FOLDER}/settings.py").run(timeout=10)

    assert not at.exception
    assert len(at.table) == 1
    assert any("runners" in w.value for w in at.warning)
    assert [s for s in at.selectbox if s.label == RUNNER_SELECT_LABEL] == []


@skip(
    ""
)  # TODO: fix this test once the issues with test_add_new_project_form_submission() are fixed
def test_add_new_settings_form_submission() -> None:
    """A test for the form submission on the settings page."""


def _settings_df_with_software() -> pd.DataFrame:
    """Get settings entries of mixed software type and runner, youngest first, as df_from_db_data sorts them."""
    return pd.DataFrame(
        {
            "_id": [1, 2, 3, 4],
            "created_at_": ["2021-01-04", "2021-01-03", "2021-01-02", "2021-01-01"],
            "name": ["young", "old", "other_type", "other_runner"],
            "version": [2, 1, 1, 1],
            "software": [
                "alphadia-2.0.0",
                "alphadia-1.10.0",
                "msqc/run_msqc.sh",
                "alphadia-on-docker",
            ],
            "software_type": ["alphadia", "alphadia", "msqc", "alphadia"],
            "runner_name": ["slurm", "slurm", "slurm", "docker"],
            "status": ["active", "inactive", "active", "active"],
        },
    )


@patch("shared.db.models.ProjectSettings.objects")
@patch("service.db.get_project_data")
@patch("service.db.get_settings_data")
@patch("service.db.df_from_db_data")
def test_settings_software_selectbox_offers_the_used_software(
    mock_df: MagicMock,
    mock_get: MagicMock,  # noqa: ARG001
    mock_project_get: MagicMock,  # noqa: ARG001
    mock_ps_objects: MagicMock,
) -> None:
    """Test that the software of the selected type and runner is offered, youngest first, archived included."""
    mock_ps_objects.all.return_value = []
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


@patch("shared.db.models.ProjectSettings.objects")
@patch("service.db.get_project_data")
@patch("service.db.get_settings_data")
@patch("service.db.df_from_db_data")
def test_settings_software_selectbox_without_used_software_offers_adding_one(
    mock_df: MagicMock,
    mock_get: MagicMock,  # noqa: ARG001
    mock_project_get: MagicMock,  # noqa: ARG001
    mock_ps_objects: MagicMock,
) -> None:
    """Test that a deployment without matching settings falls back to entering the software by hand."""
    mock_ps_objects.all.return_value = []
    mock_df.return_value = _settings_df()  # has no software_type and runner_name

    at = AppTest.from_file(f"{PAGES_FOLDER}/settings.py").run(timeout=10)

    assert not at.exception
    software_selects = [s for s in at.selectbox if s.label == SOFTWARE_SELECT_LABEL]
    assert software_selects[0].options == [ADD_NEW_SOFTWARE_OPTION]
    assert [t for t in at.text_input if t.label == SOFTWARE_SELECT_LABEL]
