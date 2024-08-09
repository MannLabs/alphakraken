"""Tests for the handler_impl module."""

from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest
from dags.impl.handler_impl import add_to_db
from plugins.common.keys import DagContext, DagParams, OpArgs, XComKeys


@patch("dags.impl.handler_impl.get_instrument_data_path")
@patch("os.stat")
@patch("dags.impl.handler_impl.add_new_raw_file_to_db")
@patch("dags.impl.handler_impl.put_xcom")
def test_add_to_db(
    mock_put_xcom: MagicMock,
    mock_add_new_raw_file_to_db: MagicMock,
    mock_stat: MagicMock,
    mock_get_instrument_data_path: MagicMock,
) -> None:
    """Test add_to_db makes the expected calls."""
    # Given
    ti = Mock()
    kwargs = {
        DagContext.PARAMS: {DagParams.RAW_FILE_NAME: "test_file.raw"},
        OpArgs.INSTRUMENT_ID: "instrument1",
    }
    mock_get_instrument_data_path.return_value = Path("/path/to/data")
    mock_stat.return_value.st_size = 42 * 1024**3

    # When
    add_to_db(ti, **kwargs)

    # Then
    mock_get_instrument_data_path.assert_called_once_with("instrument1")
    mock_add_new_raw_file_to_db.assert_called_once_with(
        "test_file.raw",
        instrument_id="instrument1",
        raw_file_size=pytest.approx(42, 0.001),
    )
    mock_put_xcom.assert_called_once_with(ti, XComKeys.RAW_FILE_NAME, "test_file.raw")
