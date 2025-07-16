"""Test the home.py file."""

from pathlib import Path

from streamlit.testing.v1 import AppTest

PAGES_FOLDER = Path(__file__).parent / Path("../../pages_")


def test_app() -> None:
    """A very boring test for a very boring page."""
    AppTest.from_file(f"{PAGES_FOLDER}/home.py").run()
