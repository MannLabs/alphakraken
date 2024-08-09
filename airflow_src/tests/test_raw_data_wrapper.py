"""Tests for the RawDataWrapper class."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Generator
from unittest.mock import MagicMock, patch

import pytest
import pytz
from common.keys import InstrumentTypes
from common.settings import INSTRUMENTS
from db.models import RawFile
from plugins.raw_data_wrapper import (
    BrukerRawDataWrapper,
    RawDataWrapper,
    ThermoRawDataWrapper,
    ZenoRawDataWrapper,
)


class TestableRawDataWrapper(RawDataWrapper):
    """A testable subclass of RawDataWrapper to test the methods provided by the abstract class."""

    main_file_extension = "test_ext"

    def _file_path_to_monitor_acquisition(self) -> Path:
        """Dummy implementation."""

    def _get_files_to_copy(self) -> dict[Path, Path]:
        """Dummy implementation."""


@pytest.mark.parametrize(
    ("instrument_type", "expected_class"),
    [
        (InstrumentTypes.THERMO, ThermoRawDataWrapper),
        (InstrumentTypes.ZENO, ZenoRawDataWrapper),
    ],
)
def test_raw_data_wrapper_instantiation(
    instrument_type: str, expected_class: type[RawDataWrapper]
) -> None:
    """Test that the correct RawDataWrapper subclass is instantiated."""
    with patch.dict(INSTRUMENTS, {"instrument1": {"type": instrument_type}}):
        wrapper = RawDataWrapper.create(instrument_id="instrument1", raw_file_name=None)
        assert isinstance(wrapper, expected_class)


@patch("plugins.raw_data_wrapper.get_internal_instrument_data_path")
def test_get_dir_contents_returns_correct_set_of_paths(
    mock_get_instrument_data_path: MagicMock,
) -> None:
    """Test that the correct set of paths is returned."""
    file_names = {"file1.test_ext", "file2.test_ext"}
    returned_paths = {Path(f"/fake/instrument/path/{f}") for f in file_names}

    mock_get_instrument_data_path.return_value.glob.return_value = list(returned_paths)

    raw_data_wrapper = TestableRawDataWrapper(
        instrument_id="instrument1", raw_file_name=None
    )

    assert raw_data_wrapper.get_raw_files_on_instrument() == file_names


@pytest.fixture()
def mock_instrument_paths() -> Generator[Path, None, None]:
    """Mock the instrument data and backup paths."""
    with patch(
        "plugins.raw_data_wrapper.get_internal_instrument_data_path"
    ) as mock_data_path, patch(
        "plugins.raw_data_wrapper.get_internal_instrument_backup_path"
    ) as mock_backup_path:
        mock_data_path.return_value = Path("/path/to/instrument")
        mock_backup_path.return_value = Path("/path/to/backup")
        yield mock_data_path, mock_backup_path


def test_raw_data_wrapper_unsupported_vendor() -> None:
    """Test that creating a wrapper for an unsupported vendor raises ValueError."""
    with patch.dict(
        INSTRUMENTS, {"instrument1": {"type": "UNSUPPORTED"}}
    ), pytest.raises(ValueError, match="Unsupported vendor: UNSUPPORTED"):
        RawDataWrapper.create(instrument_id="instrument1", raw_file_name="sample.raw")


@pytest.mark.parametrize(
    ("wrapper_class", "raw_file_name", "expected_extension"),
    [
        (ThermoRawDataWrapper, "sample.raw", ".raw"),
        (ZenoRawDataWrapper, "sample.wiff", ".wiff"),
        (BrukerRawDataWrapper, "sample.d", ".d"),
    ],
)
def test_raw_data_wrapper_file_extension_check(
    wrapper_class: type[RawDataWrapper],
    raw_file_name: str,
    expected_extension: str,
) -> None:
    """Test that the file extension check works correctly."""
    wrapper = wrapper_class("instrument1", raw_file_name=raw_file_name)
    assert wrapper._main_file_extension == expected_extension  # noqa: SLF001


def test_raw_data_wrapper_invalid_file_extension() -> None:
    """Test that initializing with an invalid file extension raises ValueError."""
    with pytest.raises(
        ValueError, match="Unsupported file extension: .txt, expected .raw"
    ):
        ThermoRawDataWrapper("instrument1", raw_file_name="sample.txt")


@patch("plugins.raw_data_wrapper.get_internal_instrument_data_path")
def test_get_raw_files_on_instrument(mock_instrument_path: MagicMock) -> None:
    """Test that get_raw_files_on_instrument returns the correct set of file names."""
    file_names = {"file1.raw", "file2.raw"}
    file_paths = {Path(f"/path/to/instrument/{f}") for f in file_names}
    mock_instrument_path.return_value.glob.return_value = file_paths

    wrapper = ThermoRawDataWrapper("instrument1")
    assert wrapper.get_raw_files_on_instrument() == file_names


@pytest.mark.parametrize(
    ("wrapper_class", "raw_file_name", "expected_watch_path"),
    [
        (ThermoRawDataWrapper, "sample.raw", Path("/path/to/instrument/sample.raw")),
        (ZenoRawDataWrapper, "sample.wiff", Path("/path/to/instrument/sample.wiff")),
        (
            BrukerRawDataWrapper,
            "sample.d",
            Path("/path/to/instrument/sample.d/analysis.tdf_bin"),
        ),
    ],
)
def test_file_path_to_monitor_acquisition(
    wrapper_class: type[RawDataWrapper],
    raw_file_name: str,
    expected_watch_path: Path,
    mock_instrument_paths: MagicMock,  # noqa: ARG001
) -> None:
    """Test that file_path_to_monitor_acquisition returns the correct path for each wrapper type."""
    wrapper = wrapper_class("instrument1", raw_file_name=raw_file_name)
    assert wrapper.file_path_to_monitor_acquisition() == expected_watch_path


def test_thermo_get_files_to_copy(
    mock_instrument_paths: MagicMock,  # noqa: ARG001
) -> None:
    """Test that get_files_to_copy returns the correct mapping for ThermoRawDataWrapper."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="123---sample.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        original_name="sample.raw",
    )

    wrapper = ThermoRawDataWrapper("instrument1", raw_file=mock_raw_file)
    expected_mapping = {
        Path("/path/to/instrument/sample.raw"): Path(
            "/path/to/backup/1970_01/123---sample.raw"
        )
    }
    assert wrapper.get_files_to_copy() == expected_mapping


@patch("plugins.raw_data_wrapper.get_internal_instrument_data_path")
def test_zeno_get_files_to_copy(mock_instrument_path: MagicMock) -> None:
    """Test that get_files_to_copy returns the correct mapping for ZenoRawDataWrapper."""
    mock_instrument_path.return_value.glob.return_value = [
        Path("/path/to/instrument/sample.wiff"),
        Path("/path/to/instrument/sample.wiff.scan"),
    ]

    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="123---sample.wiff",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        original_name="sample.wiff",
    )

    wrapper = ZenoRawDataWrapper("instrument1", raw_file=mock_raw_file)
    expected_mapping = {
        Path("/path/to/instrument/sample.wiff"): Path(
            "/opt/airflow/mounts/backup/instrument1/1970_01/123---sample.wiff"
        ),
        Path("/path/to/instrument/sample.wiff.scan"): Path(
            "/opt/airflow/mounts/backup/instrument1/1970_01/123---sample.wiff.scan"
        ),
    }
    assert wrapper.get_files_to_copy() == expected_mapping
    mock_instrument_path.return_value.glob.assert_called_once_with("sample.*")


@patch("plugins.raw_data_wrapper.get_internal_instrument_data_path")
def test_bruker_get_files_to_copy(mock_instrument_path: MagicMock) -> None:
    """Test that get_files_to_copy returns the correct mapping for BrukerRawDataWrapper."""
    mock_output_path = MagicMock()
    mock_instrument_path.return_value.__truediv__.return_value = mock_output_path

    mp1 = MagicMock(wraps=Path("/path/to/instrument/sample.d/file1.txt"))
    mp1.is_file.return_value = True
    mp1.relative_to.return_value = Path("sample.d/file1.txt")
    mp2 = MagicMock(wraps=Path("/path/to/instrument/sample.d/subdir/file2.txt"))
    mp2.is_file.return_value = True
    mp2.relative_to.return_value = Path("sample.d/subdir/file2.txt")

    mock_output_path.rglob.return_value = [mp1, mp2]

    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="123---sample.d",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        original_name="sample.d",
    )

    wrapper = BrukerRawDataWrapper("instrument1", raw_file=mock_raw_file)
    expected_mapping = {
        mp1: Path(
            "/opt/airflow/mounts/backup/instrument1/1970_01/123---sample.d/file1.txt"
        ),
        mp2: Path(
            "/opt/airflow/mounts/backup/instrument1/1970_01/123---sample.d/subdir/file2.txt"
        ),
    }
    assert wrapper.get_files_to_copy() == expected_mapping
    mock_output_path.rglob.assert_called_once_with("*")
