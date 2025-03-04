"""Tests for the RawFileWrapperFactory class."""
# ruff: noqa: SLF001

from __future__ import annotations

import tempfile
from collections.abc import Generator
from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import pytz
from common.keys import InstrumentTypes
from common.yaml import INSTRUMENTS
from plugins.raw_file_wrapper_factory import (
    BrukerRawFileMonitorWrapper,
    BrukerRawFileWriteWrapper,
    CopyPathProvider,
    MovePathProvider,
    RawFileMonitorWrapper,
    RawFileWrapperFactory,
    RawFileWriteWrapper,
    RemovePathProvider,
    SciexRawFileMonitorWrapper,
    SciexRawFileWriteWrapper,
    ThermoRawFileMonitorWrapper,
    ThermoRawFileWriteWrapper,
)

from shared.db.models import RawFile


class TestableRawFileMonitorWrapper(RawFileMonitorWrapper):
    """A testable subclass of RawFileMonitorWrapper to test the methods provided by the abstract class."""

    _raw_file_extension = "test_ext"

    def _file_path_to_monitor_acquisition(self) -> Any:  # noqa: ANN401
        """Dummy implementation."""

    def _get_files_to_copy(self) -> Any:  # noqa: ANN401
        """Dummy implementation."""


class TestableRawFileWriteWrapper(RawFileWriteWrapper):
    """A testable subclass of RawFileWriteWrapper to test the methods provided by the abstract class."""

    def _get_files_to_copy(self) -> Any:  # noqa: ANN401
        """Dummy implementation."""

    def _get_files_to_move(self) -> Any:  # noqa: ANN401
        """Dummy implementation."""

    def _get_folder_to_remove(self) -> Any:  # noqa: ANN401
        """Dummy implementation."""


@patch("plugins.raw_file_wrapper_factory.RawFileWrapperFactory")
def test_raw_file_wrapper_check_path_provider_copy(
    mock_raw_file_wrapper_factory: MagicMock,  # noqa: ARG001
) -> None:
    """Test that the path provider is correctly checked."""
    with patch.dict(INSTRUMENTS, {"instrument1": {"type": "bruker"}}):
        wrapper = TestableRawFileWriteWrapper(
            "instrument1", raw_file=MagicMock(), path_provider=CopyPathProvider
        )

    with pytest.raises(TypeError):
        wrapper.get_files_to_remove()

    with pytest.raises(TypeError):
        wrapper.get_files_to_move()

    wrapper.get_files_to_copy()
    # ok: nothing raised


@patch("plugins.raw_file_wrapper_factory.RawFileWrapperFactory")
def test_raw_file_wrapper_check_path_provider_move(
    mock_raw_file_wrapper_factory: MagicMock,  # noqa: ARG001
) -> None:
    """Test that the path provider is correctly checked."""
    with patch.dict(INSTRUMENTS, {"instrument1": {"type": "bruker"}}):
        wrapper = TestableRawFileWriteWrapper(
            "instrument1", raw_file=MagicMock(), path_provider=MovePathProvider
        )

    with pytest.raises(TypeError):
        wrapper.get_files_to_remove()

    wrapper.get_files_to_move()
    # ok: nothing raised

    with pytest.raises(TypeError):
        wrapper.get_files_to_copy()


@patch("plugins.raw_file_wrapper_factory.RawFileWrapperFactory")
def test_raw_file_wrapper_check_path_provider_remove(
    mock_raw_file_wrapper_factory: MagicMock,  # noqa: ARG001
) -> None:
    """Test that the path provider is correctly checked."""
    with patch.dict(INSTRUMENTS, {"instrument1": {"type": "bruker"}}):
        wrapper = TestableRawFileWriteWrapper(
            "instrument1", raw_file=MagicMock(), path_provider=RemovePathProvider
        )

    wrapper.get_files_to_remove()
    # ok: nothing raised

    with pytest.raises(TypeError):
        wrapper.get_files_to_move()

    with pytest.raises(TypeError):
        wrapper.get_files_to_copy()


@patch("plugins.raw_file_wrapper_factory.get_internal_instrument_data_path")
def test_get_dir_contents_returns_correct_set_of_paths(
    mock_get_internal_instrument_data_path: MagicMock,
) -> None:
    """Test that the correct set of paths is returned."""
    file_names = {"file1.test_ext", "file2.test_ext"}
    returned_paths = {Path(f"/fake/instrument/path/{f}") for f in file_names}

    mock_get_internal_instrument_data_path.return_value.glob.return_value = list(
        returned_paths
    )

    raw_file_monitor_wrapper = TestableRawFileMonitorWrapper(
        instrument_id="instrument1"
    )

    assert raw_file_monitor_wrapper.get_raw_files_on_instrument() == file_names


@pytest.mark.parametrize(
    ("instrument_type", "extension", "expected_class"),
    [
        (InstrumentTypes.THERMO, ".raw", ThermoRawFileMonitorWrapper),
        (InstrumentTypes.SCIEX, ".wiff", SciexRawFileMonitorWrapper),
        (InstrumentTypes.BRUKER, ".d", BrukerRawFileMonitorWrapper),
    ],
)
def test_raw_file_wrapper_factory_instantiation_monitors(
    instrument_type: str, extension: str, expected_class: type[RawFileWrapperFactory]
) -> None:
    """Test that the correct RawFileWrapperFactory subclass is instantiated."""
    with patch.dict(INSTRUMENTS, {"instrument1": {"type": instrument_type}}):
        wrapper = RawFileWrapperFactory.create_monitor_wrapper(
            instrument_id="instrument1", raw_file_original_name=f"some_file{extension}"
        )
        assert isinstance(wrapper, expected_class)


@pytest.fixture
def mock_raw_file() -> RawFile:
    """Fixture for a mock RawFile."""
    return RawFile(
        id="123-original_file.raw",
        original_name="original_file.raw",
        created_at=datetime(2023, 1, 1, tzinfo=pytz.UTC),
        instrument_id="instrument1",
    )


def test_copy_path_provider(mock_raw_file: RawFile) -> None:
    """Test the CopyPathProvider class."""
    provider = CopyPathProvider("instrument1", mock_raw_file)

    assert provider.get_source_folder_path() == Path(
        "/opt/airflow/mounts/instruments/instrument1"
    )
    assert provider.get_target_folder_path() == Path(
        "/opt/airflow/mounts/backup/instrument1/2023_01"
    )
    assert provider.get_source_file_name() == "original_file.raw"
    assert provider.get_target_file_name() == "123-original_file.raw"


def test_move_path_provider(mock_raw_file: RawFile) -> None:
    """Test the MovePathProvider class."""
    provider = MovePathProvider("instrument1", mock_raw_file)

    assert provider.get_source_folder_path() == Path(
        "/opt/airflow/mounts/instruments/instrument1"
    )
    assert provider.get_target_folder_path() == Path(
        "/opt/airflow/mounts/instruments/instrument1/Backup"
    )
    assert provider.get_source_file_name() == "original_file.raw"
    assert provider.get_target_file_name() == "123-original_file.raw"


def test_remove_path_provider(mock_raw_file: RawFile) -> None:
    """Test the RemovePathProvider class."""
    provider = RemovePathProvider("instrument1", mock_raw_file)

    assert provider.get_source_folder_path() == Path(
        "/opt/airflow/mounts/instruments/instrument1/Backup"
    )
    assert provider.get_target_folder_path() == Path(
        "/opt/airflow/mounts/backup/instrument1/2023_01"
    )
    assert provider.get_source_file_name() == "123-original_file.raw"
    assert provider.get_target_file_name() == "123-original_file.raw"


@pytest.mark.parametrize(
    ("instrument_type", "expected_class"),
    [
        (InstrumentTypes.THERMO, ThermoRawFileWriteWrapper),
        (InstrumentTypes.SCIEX, SciexRawFileWriteWrapper),
        (InstrumentTypes.BRUKER, BrukerRawFileWriteWrapper),
    ],
)
@patch("plugins.raw_file_wrapper_factory.RawFileWrapperFactory.create_monitor_wrapper")
def test_raw_file_wrapper_factory_instantiation_copier(
    mock_create_monitor_wrapper: MagicMock,  # noqa: ARG001
    mock_raw_file: MagicMock,
    instrument_type: str,
    expected_class: type[RawFileWrapperFactory],
) -> None:
    """Test that the correct RawFileWrapperFactory subclass is instantiated."""
    with patch.dict(INSTRUMENTS, {"instrument1": {"type": instrument_type}}):
        wrapper = RawFileWrapperFactory.create_write_wrapper(
            raw_file=mock_raw_file,
            path_provider=CopyPathProvider,
        )
        assert isinstance(wrapper, expected_class)

        assert wrapper._source_folder_path == Path(
            "/opt/airflow/mounts/instruments/instrument1"
        )
        assert wrapper._target_folder_path == Path(
            "/opt/airflow/mounts/backup/instrument1/2023_01"
        )
        assert wrapper._source_file_name == "original_file.raw"
        assert wrapper._target_file_name == "123-original_file.raw"


@pytest.fixture
def mock_instrument_paths() -> Generator[Path, None, None]:
    """Mock the instrument data and backup paths."""
    with (
        patch(
            "plugins.raw_file_wrapper_factory.get_internal_instrument_data_path"
        ) as mock_data_path,
        patch(
            "plugins.raw_file_wrapper_factory.get_internal_backup_path_for_instrument"
        ) as mock_backup_path,
    ):
        mock_data_path.return_value = Path("/path/to/instrument")
        mock_backup_path.return_value = Path("/path/to/backup")
        yield mock_data_path, mock_backup_path


def test_raw_file_wrapper_factory_unsupported_vendor() -> None:
    """Test that creating a wrapper for an unsupported vendor raises ValueError."""
    with (
        patch.dict(INSTRUMENTS, {"instrument1": {"type": "UNSUPPORTED"}}),
        pytest.raises(
            ValueError,
            match="Unsupported vendor or handler type for instrument1: UNSUPPORTED, monitor",
        ),
    ):
        RawFileWrapperFactory.create_monitor_wrapper(
            instrument_id="instrument1", raw_file_original_name="sample.raw"
        )


@pytest.mark.parametrize(
    ("wrapper_class", "raw_file_name", "expected_extension"),
    [
        (ThermoRawFileMonitorWrapper, "sample.raw", ".raw"),
        (SciexRawFileMonitorWrapper, "sample.wiff", ".wiff"),
        (BrukerRawFileMonitorWrapper, "sample.d", ".d"),
    ],
)
def test_raw_file_wrapper_factory_file_extension_check(
    wrapper_class: type[RawFileMonitorWrapper],
    raw_file_name: str,
    expected_extension: str,
) -> None:
    """Test that the file extension check works correctly."""
    wrapper = wrapper_class("instrument1", raw_file_original_name=raw_file_name)
    assert wrapper._raw_file_extension == expected_extension


def test_raw_file_wrapper_factory_invalid_file_extension() -> None:
    """Test that initializing with an invalid file extension raises ValueError."""
    with pytest.raises(
        ValueError, match="Unsupported file extension: .txt, expected .raw"
    ):
        ThermoRawFileMonitorWrapper("instrument1", raw_file_original_name="sample.txt")


@patch("plugins.raw_file_wrapper_factory.get_internal_instrument_data_path")
def test_get_raw_files_on_instrument(mock_instrument_path: MagicMock) -> None:
    """Test that get_raw_files_on_instrument returns the correct set of file names."""
    file_names = {"file1.raw", "file2.raw"}
    file_paths = {Path(f"/path/to/instrument/{f}") for f in file_names}
    mock_instrument_path.return_value.glob.return_value = file_paths

    wrapper = ThermoRawFileMonitorWrapper("instrument1")
    assert wrapper.get_raw_files_on_instrument() == file_names


@pytest.mark.parametrize(
    ("wrapper_class", "raw_file_name", "expected_watch_path"),
    [
        (
            ThermoRawFileMonitorWrapper,
            "sample.raw",
            Path("/path/to/instrument/sample.raw"),
        ),
        (
            SciexRawFileMonitorWrapper,
            "sample.wiff",
            Path("/path/to/instrument/sample.wiff"),
        ),
        (
            BrukerRawFileMonitorWrapper,
            "sample.d",
            Path("/path/to/instrument/sample.d/analysis.tdf_bin"),
        ),
    ],
)
def test_file_path_to_monitor_acquisition_pass_raw_file_original_name(
    wrapper_class: type[RawFileMonitorWrapper],
    raw_file_name: str,
    expected_watch_path: Path,
    mock_instrument_paths: MagicMock,  # noqa: ARG001
) -> None:
    """Test that file_path_to_monitor_acquisition returns the correct path for each wrapper type."""
    wrapper = wrapper_class("instrument1", raw_file_original_name=raw_file_name)
    assert wrapper.file_path_to_monitor_acquisition() == expected_watch_path


@pytest.mark.parametrize(
    ("wrapper_class", "raw_file", "expected_watch_path"),
    [
        (
            ThermoRawFileMonitorWrapper,
            MagicMock(wraps=RawFile, original_name="sample.raw"),
            Path("/path/to/instrument/sample.raw"),
        ),
        (
            SciexRawFileMonitorWrapper,
            MagicMock(wraps=RawFile, original_name="sample.wiff"),
            Path("/path/to/instrument/sample.wiff"),
        ),
        (
            BrukerRawFileMonitorWrapper,
            MagicMock(wraps=RawFile, original_name="sample.d"),
            Path("/path/to/instrument/sample.d/analysis.tdf_bin"),
        ),
    ],
)
def test_file_path_to_monitor_acquisition_pass_raw_file(
    wrapper_class: type[RawFileMonitorWrapper],
    raw_file: RawFile,
    expected_watch_path: Path,
    mock_instrument_paths: MagicMock,  # noqa: ARG001
) -> None:
    """Test that file_path_to_monitor_acquisition returns the correct path for each wrapper type."""
    wrapper = wrapper_class("instrument1", raw_file=raw_file)
    assert wrapper.file_path_to_monitor_acquisition() == expected_watch_path


def test_file_path_to_monitor_acquisition_pass_both_raises() -> None:
    """Test that file_path_to_monitor_acquisition raises if it get both raw_file arguments."""
    raw_file = MagicMock(wraps=RawFile, original_name="sample.raw")

    with pytest.raises(
        ValueError,
        match="Either raw_file or raw_file_original_name should be set, not both.",
    ):
        ThermoRawFileMonitorWrapper(
            "instrument1", raw_file=raw_file, raw_file_original_name="sample.raw"
        )


@patch("plugins.raw_file_wrapper_factory.RawFileWrapperFactory.create_monitor_wrapper")
def test_thermo_get_files_to_copy(
    mock_create_monitor_wrapper: MagicMock,
    mock_instrument_paths: MagicMock,  # noqa: ARG001
) -> None:
    """Test that get_files_to_copy returns the correct mapping for ThermoRawDataWrapper."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="123-sample.raw",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        original_name="sample.raw",
    )

    wrapper = ThermoRawFileWriteWrapper(
        "instrument1", raw_file=mock_raw_file, path_provider=CopyPathProvider
    )
    expected_mapping = {
        Path("/path/to/instrument/sample.raw"): Path(
            "/path/to/backup/1970_01/123-sample.raw"
        )
    }
    assert wrapper.get_files_to_copy() == expected_mapping
    mock_create_monitor_wrapper.assert_called_once_with("instrument1", mock_raw_file)


@patch("plugins.raw_file_wrapper_factory.RawFileWrapperFactory.create_monitor_wrapper")
@patch("plugins.raw_file_wrapper_factory.get_internal_instrument_data_path")
def test_sciex_get_files_to_copy(
    mock_instrument_path: MagicMock,
    mock_create_monitor_wrapper: MagicMock,
) -> None:
    """Test that get_files_to_copy returns the correct mapping for SciexRawDataWrapper."""
    mock_instrument_path.return_value.glob.return_value = [
        Path("/path/to/instrument/sample.wiff"),
        Path("/path/to/instrument/sample.wiff.scan"),
    ]

    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="123-sample.wiff",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        original_name="sample.wiff",
    )

    wrapper = SciexRawFileWriteWrapper(
        "instrument1", raw_file=mock_raw_file, path_provider=CopyPathProvider
    )
    expected_mapping = {
        Path("/path/to/instrument/sample.wiff"): Path(
            "/opt/airflow/mounts/backup/instrument1/1970_01/123-sample.wiff"
        ),
        Path("/path/to/instrument/sample.wiff.scan"): Path(
            "/opt/airflow/mounts/backup/instrument1/1970_01/123-sample.wiff.scan"
        ),
    }

    # when
    assert wrapper.get_files_to_copy() == expected_mapping
    mock_instrument_path.return_value.glob.assert_called_once_with("sample.*")

    mock_create_monitor_wrapper.assert_called_once_with("instrument1", mock_raw_file)


def test_bruker_get_files_to_copy() -> None:
    """Test that get_files_to_copy returns the correct mapping for BrukerRawDataWrapper."""
    mock_raw_file = MagicMock(
        wraps=RawFile,
        id="123-sample.d",
        created_at=datetime.fromtimestamp(0, tz=pytz.UTC),
        original_name="sample.d",
    )

    # using a tempdir here as the path manipulations are nontrivial and mocking them would reduce test scope
    with tempfile.TemporaryDirectory() as tempdir:
        instrument_path = Path(tempdir) / "instrument1"
        raw_file_path = instrument_path / "sample.d"
        raw_file_path.mkdir(parents=True)
        (raw_file_path / "file1.txt").touch()
        (raw_file_path / "subdir.m").mkdir()
        (raw_file_path / "subdir.m" / "file2.txt").touch()

        # Define mock functions for patching
        def mock_get_internal_instrument_data_path(instrument_id: str) -> Path:
            """Mock method for get_internal_instrument_data_path()."""
            return Path(tempdir) / instrument_id

        def mock_get_internal_backup_path_for_instrument(instrument_id: str) -> Path:
            """Mock method for get_internal_backup_path_for_instrument()."""
            return Path(tempdir) / "backup" / instrument_id

        with (
            patch(
                "plugins.raw_file_wrapper_factory.get_internal_instrument_data_path",
                side_effect=mock_get_internal_instrument_data_path,
            ),
            patch(
                "plugins.raw_file_wrapper_factory.get_internal_backup_path_for_instrument",
                side_effect=mock_get_internal_backup_path_for_instrument,
            ),
            patch.dict(INSTRUMENTS, {"instrument1": {"type": "bruker"}}),
        ):
            wrapper = BrukerRawFileWriteWrapper(
                "instrument1", raw_file=mock_raw_file, path_provider=CopyPathProvider
            )

            # when
            files_to_copy = wrapper.get_files_to_copy()

            expected_dst_path = (
                Path(tempdir) / "backup" / "instrument1" / "1970_01" / "123-sample.d"
            )
            expected_mapping = {
                raw_file_path / "file1.txt": expected_dst_path / "file1.txt",
                raw_file_path / "subdir.m" / "file2.txt": expected_dst_path
                / "subdir.m"
                / "file2.txt",
            }

            assert files_to_copy == expected_mapping
