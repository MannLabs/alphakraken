"""Tests for the overview_utils module."""

from datetime import datetime, timedelta

import pandas as pd
from pages_.impl.overview_utils import add_eta

NOW = datetime(2024, 7, 1, 12, 0, 0)  # noqa: DTZ001


def test_add_eta_for_pending_file() -> None:
    """Test that the ETA of a file that is still being processed is in the future."""
    df = pd.DataFrame(
        {
            "status": ["quanting"],
            "created_at_": [NOW - timedelta(minutes=1)],
        }
    )

    # when
    result = add_eta(df, NOW, lag_time=120)

    assert result.tolist() == ["in 1m"]


def test_add_eta_for_overdue_file() -> None:
    """Test that the ETA of a file that is overdue is reported as 'now'."""
    df = pd.DataFrame(
        {
            "status": ["quanting"],
            "created_at_": [NOW - timedelta(minutes=5)],
        }
    )

    # when
    result = add_eta(df, NOW, lag_time=120)

    assert result.tolist() == ["now (-1 days +23:57:00)"]


def test_add_eta_skips_terminal_files() -> None:
    """Test that files in a terminal status get no ETA."""
    df = pd.DataFrame({"status": ["done"], "created_at_": [NOW]})

    # when
    result = add_eta(df, NOW, lag_time=120)

    assert result.empty
