"""Test the overview.py file."""

from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest
from service.session_state import SessionStateKeys
from streamlit.testing.v1 import AppTest

from shared.keys import MetricsTypes
from shared.tests.factories import kraken_status, metrics, raw_file

PAGES_FOLDER = Path(__file__).parent / Path("../../pages_")


TS1 = datetime(2024, 1, 2, 12, 0, 0, tzinfo=UTC)
TS2 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)


@pytest.fixture(autouse=True)
def _db() -> Iterator[None]:
    """Make the database hold two raw files with their metrics, and one healthy instrument."""
    alphadia_values = {
        "proteins": 1,
        "precursors": 1,
        "ms1_accuracy": 1.0,
        "fwhm_rt": 1.0,
        "time_elapsed": 60.0,
        "weighted_ms1_intensity_sum": 1.0,
        "intensity_sum": 1.0,
    }
    with (
        patch("shared.db.raw_files.find") as mock_find,
        patch("shared.db.metrics.latest_by_raw_file") as mock_latest,
        patch("shared.db.status.find", return_value=[kraken_status(id="i1")]),
    ):
        mock_find.return_value = [
            raw_file(
                id="f1",
                original_name="f1",
                instrument_id="i1",
                project_id="P1",
                status="done",
                status_details="",
                size=1024**3,
                created_at=TS1,
                created_at_=TS1,
                updated_at_=TS1,
            ),
            raw_file(
                id="f2",
                original_name="f2",
                instrument_id="i1",
                project_id="P2",
                status="error",
                status_details="",
                size=2 * 1024**3,
                created_at=TS2,
                created_at_=TS2,
                updated_at_=TS2,
            ),
        ]
        mock_latest.return_value = {
            "f1": {
                MetricsTypes.ALPHADIA: metrics(
                    settings_version=1,
                    relative_output_path=None,
                    output_display_base_path=None,
                    values=alphadia_values,
                ),
                MetricsTypes.CUSTOM: metrics(
                    type=MetricsTypes.CUSTOM,
                    relative_output_path=None,
                    output_display_base_path=None,
                    values={"proteins": 1, "some_custom_metric": 1},
                ),
            },
            "f2": {
                MetricsTypes.ALPHADIA: metrics(
                    settings_version=2,
                    relative_output_path=None,
                    output_display_base_path=None,
                    values={
                        k: v * 2 if isinstance(v, int) else v * 2.0
                        for k, v in alphadia_values.items()
                    },
                ),
                MetricsTypes.CUSTOM: metrics(
                    type=MetricsTypes.CUSTOM,
                    relative_output_path=None,
                    output_display_base_path=None,
                    values={"proteins": 2, "some_custom_metric": 2},
                ),
            },
            # metrics of a raw file that was not selected are discarded
            "other": {
                MetricsTypes.ALPHADIA: metrics(
                    relative_output_path=None,
                    output_display_base_path=None,
                    values={"proteins": 99},
                )
            },
        }
        yield


def test_overview() -> None:
    """Test that overview page renders successfully."""
    # when
    at = AppTest.from_file(f"{PAGES_FOLDER}/overview.py").run()

    # then
    ts1str = TS1.strftime("%Y-%m-%d %H:%M:%S")
    ts2str = TS2.strftime("%Y-%m-%d %H:%M:%S")

    expected_data = {
        "id": {"f1": "f1", "f2": "f2"},
        "original_name": {"f1": "f1", "f2": "f2"},
        "raw_file": {"f1": "f1", "f2": "f2"},
        "alphadia__proteins": {"f1": 1, "f2": 2},
        "alphadia__precursors": {"f1": 1, "f2": 2},
        "alphadia__calibration_ms1_bias": {"f1": 1.0, "f2": 2.0},
        "alphadia__fwhm_rt": {"f1": 1.0, "f2": 2.0},
        "created_at": {"f1": TS1, "f2": TS2},
        "created_at_": {"f1": TS1, "f2": TS2},
        "updated_at_": {"f1": TS1, "f2": TS2},
        "file_created": {"f1": ts1str, "f2": ts2str},
        "alphadia__time_elapsed_minutes": {"f1": 1.0, "f2": 2.0},
        "size_gb": {"f1": 1.0, "f2": 2.0},
        "project_id": {"f1": "P1", "f2": "P2"},
        "status": {"f1": "done", "f2": "error"},
        "status_details": {"f1": "", "f2": ""},
        "instrument_id": {"f1": "i1", "f2": "i1"},
        "instrument_file_status": {"f1": "n/a", "f2": "n/a"},
        "alphadia__weighted_ms1_intensity_sum": {"f1": 1.0, "f2": 2.0},
        "alphadia__pg_intensity_sum": {"f1": 1.0, "f2": 2.0},
        "is_baseline": {"f1": False, "f2": False},
        "alphadia__settings_name": {"f1": "fallback", "f2": "fallback"},
        "alphadia__settings_version": {"f1": 1, "f2": 2},
        "custom__proteins": {"f1": 1, "f2": 2},
        "custom__some_custom_metric": {"f1": 1, "f2": 2},
        "custom__settings_name": {"f1": "fallback", "f2": "fallback"},
        "custom__settings_version": {"f1": 1, "f2": 1},
    }

    assert not at.exception
    assert at.dataframe[0].value.to_dict() == expected_data
    # plots not tested


def test_overview_file_selection_mode() -> None:
    """Test that enabling the 'File selection' toggle shows the file list and its buttons."""
    # when: the file selection toggle is enabled
    at = AppTest.from_file(f"{PAGES_FOLDER}/overview.py")
    at.session_state["file_selection_mode_widget_key"] = True
    at.run()

    # then: the file selection UI is shown instead of the table/plots
    assert not at.exception
    assert at.toggle[0].value
    assert any("File selection" in md.value for md in at.markdown)

    button_labels = [b.label for b in at.button]
    assert "🔗 Show file paths" in button_labels
    assert "🔤 Show file names" in button_labels
    assert "📁 Show output folders" in button_labels


def test_overview_project_id_filter() -> None:
    """Test that selecting a project ID narrows the table to that project only."""
    # when: the project ID "P1" is selected
    at = AppTest.from_file(f"{PAGES_FOLDER}/overview.py")
    at.session_state["project_id_widget_key"] = "P1"
    at.run()

    # then: the dropdown offers all-projects plus both projects, and only P1 is shown
    assert not at.exception
    assert at.selectbox(key="project_id_widget_key").options == ["", "P1", "P2"]
    assert at.dataframe[0].value["project_id"].to_dict() == {"f1": "P1"}


def test_overview_csv_download_is_prepared_on_demand() -> None:
    """Test that the CSV is created on demand only and discarded once the selection changes."""
    at = AppTest.from_file(f"{PAGES_FOLDER}/overview.py").run()

    # then: nothing is generated before the user asks for it
    assert not at.exception
    assert "🧮 Prepare download of filtered table (2 entries)" in [
        b.label for b in at.button
    ]
    assert at.get("download_button") == []

    # AppTest reruns the whole script rather than only the fragment, so the first-run guard
    # of the page has to be re-armed to get the table displayed again after a click.
    at.session_state[SessionStateKeys.IS_FIRST_RUN] = True

    # when: the CSV is prepared
    prepare_button = next(
        b for b in at.button if b.label.startswith("🧮 Prepare download of filtered")
    )
    prepare_button.click().run()

    # then: it is served from the media store, not by a deferred callable (which would 404)
    assert not at.exception
    download_button = at.get("download_button")[0].proto
    assert download_button.label == "⬇️ Download filtered table (2 entries)"
    assert download_button.url.endswith(".csv")
    assert not download_button.deferred_file_id

    # when: the selection changes
    at.session_state[SessionStateKeys.IS_FIRST_RUN] = True
    at.selectbox(key="project_id_widget_key").select("P1").run()

    # then: the prepared CSV no longer matches the selection and is discarded
    assert not at.exception
    assert at.get("download_button") == []
