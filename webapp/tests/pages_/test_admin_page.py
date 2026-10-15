"""Test the admin.py file."""

from pathlib import Path

from streamlit.testing.v1 import AppTest

PAGES_FOLDER = Path(__file__).parent / Path("../../pages_")


def test_app() -> None:
    """Test that the admin page renders."""
    at = AppTest.from_file(f"{PAGES_FOLDER}/admin.py").run()

    assert not at.exception
