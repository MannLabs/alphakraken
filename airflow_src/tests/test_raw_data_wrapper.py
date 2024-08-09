"""Tests for the RawDataWrapper class."""

from pathlib import Path
from unittest.mock import MagicMock, patch

from common.keys import InstrumentTypes
from common.settings import INSTRUMENTS
from plugins.raw_data_wrapper import RawDataWrapper, ThermoRawDataWrapper


class TestableRawDataWrapper(RawDataWrapper):
    """A testable subclass of RawDataWrapper to test the methods provided by the abstract class."""

    main_file_extension = "test_ext"

    def _file_path_to_watch(self) -> Path:
        """Dummy implementation."""

    def _get_files_to_copy(self) -> dict[Path, Path]:
        """Dummy implementation."""


@patch.dict(INSTRUMENTS, {"instrument1": {"type": InstrumentTypes.THERMO}})
def test_raw_data_wrapper_instantiation() -> None:
    """Test that the correct RawDataWrapper subclass is instantiated."""
    # when
    wrapper = RawDataWrapper.create(
        instrument_id="instrument1", raw_file_name="sample.raw"
    )

    assert isinstance(wrapper, ThermoRawDataWrapper)


@patch("plugins.raw_data_wrapper.get_internal_instrument_data_path")
def test_get_dir_contents_returns_correct_set_of_paths(
    mock_get_instrument_data_path: MagicMock,
) -> None:
    """Test that the correct set of paths is returned."""
    expected_set = {
        Path("/fake/instrument/path/file1.test_ext"),
        Path("/fake/instrument/path/file2.test_ext"),
    }

    mock_get_instrument_data_path.return_value.glob.return_value = list(expected_set)

    raw_data_wrapper = TestableRawDataWrapper(
        instrument_id="instrument1", raw_file_name=None
    )

    assert raw_data_wrapper.get_dir_contents() == expected_set


@patch("plugins.raw_data_wrapper.get_internal_instrument_data_path")
def test_thermo_file_path_to_watch(mock_instrument_path: MagicMock) -> None:
    """Test that the file path to watch is correctly determined."""
    mock_instrument_path.return_value = Path("/path/to/instrument1")

    # when
    thermo_wrapper = ThermoRawDataWrapper("instrument1", "sample.raw")

    expected_path = Path("/path/to/instrument1/sample.raw")

    assert thermo_wrapper.file_path_to_watch() == expected_path


@patch("plugins.raw_data_wrapper.get_internal_instrument_data_path")
@patch("plugins.raw_data_wrapper.get_internal_instrument_backup_path")
def test_thermo_get_files_to_copy(
    mock_backup_path: MagicMock, mock_instrument_path: MagicMock
) -> None:
    """Test that the files to copy are correctly determined."""
    mock_instrument_path.return_value = Path("/path/to/instrument1")
    mock_backup_path.return_value = Path("/path/to/backup")

    # when
    thermo_wrapper = ThermoRawDataWrapper("instrument1", "sample.raw")

    expected_mapping = {
        Path("/path/to/instrument1/sample.raw"): Path("/path/to/backup/sample.raw")
    }
    assert thermo_wrapper.get_files_to_copy() == expected_mapping
