"""Tests for the webapp entry point."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from service.session_state import SessionStateKeys
from streamlit.testing.v1 import AppTest

WEBAPP_FILE = Path(__file__).parent / Path("../webapp.py")
ADMIN_PAGE_TITLE = "🔧 Admin"


@pytest.mark.parametrize("is_admin", [False, True])
@patch("streamlit.navigation")
def test_admin_page_registered_only_for_admins(
    mock_navigation: MagicMock, *, is_admin: bool
) -> None:
    """Test that the admin page is part of the navigation only for admins."""
    at = AppTest.from_file(str(WEBAPP_FILE))
    if is_admin:
        at.session_state[SessionStateKeys.IS_ADMIN] = True

    at.run()

    assert not at.exception
    pages = mock_navigation.call_args.args[0]
    assert (ADMIN_PAGE_TITLE in [p.title for p in pages]) == is_admin
