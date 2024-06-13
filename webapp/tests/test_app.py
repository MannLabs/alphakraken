"""Test the app.py file."""

from pathlib import Path

from streamlit.testing.v1 import AppTest

APP_FOLDER = Path(__file__).parent / Path("../")


def test_app() -> None:
    """A very boring test for a very boring page."""
    AppTest.from_file(f"{APP_FOLDER}/app.py").run()
