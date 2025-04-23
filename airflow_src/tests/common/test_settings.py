"""Tests for the settings module."""

from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest
from common.settings import _load_alphakraken_yaml, get_instrument_settings, get_path


@patch("common.settings.Path")
def test_loads_alphakraken_yaml_successfully(mock_path: MagicMock) -> None:
    """Test that the settings are loaded successfully from a YAML file."""
    env_name = "production"

    yaml_content = """
    instruments:
      instrument1:
        type: thermo
        skip_quanting: true
        min_free_space_gb: 123
    """

    mock_path.return_value.__truediv__.return_value.open = mock_open(
        read_data=yaml_content
    )

    # when
    settings = _load_alphakraken_yaml(env_name)

    assert settings == {
        "instruments": {
            "instrument1": {
                "type": "thermo",
                "skip_quanting": True,
                "min_free_space_gb": 123,
            }
        }
    }


@patch("common.settings.Path")
def test_raises_file_not_found_error_if_file_does_not_exist(
    mock_path: MagicMock,
) -> None:
    """Test that a FileNotFoundError is raised if the settings file does not exist."""
    env_name = "production"

    # when
    mock_path.return_value.__truediv__.return_value.exists.return_value = False

    with pytest.raises(FileNotFoundError):
        _load_alphakraken_yaml(env_name)


def test_returns_test_settings_for_test_environment() -> None:
    """Test that test settings are returned when the environment is set to test."""
    env_name = "_test_"

    # when
    settings = _load_alphakraken_yaml(env_name)

    assert settings == {"instruments": {"_test1_": {"type": "thermo"}}}


def test_get_instrument_settings_returns_setting_for_existing_instrument_and_key() -> (
    None
):
    """Test that a setting is returned for an existing instrument and key."""
    with (
        patch("common.settings._INSTRUMENTS", {"instrument1": {"key1": "value1"}}),
        patch("common.settings.INSTRUMENT_SETTINGS_DEFAULTS", {}),
    ):
        assert get_instrument_settings("instrument1", "key1") == "value1"


def test_get_instrument_settings_raises_key_error_for_non_existing_key() -> None:
    """Test that a KeyError is raised if the key does not exist in the instrument settings."""
    with (
        patch("common.settings._INSTRUMENTS", {"instrument1": {"key1": "value1"}}),
        patch("common.settings.INSTRUMENT_SETTINGS_DEFAULTS", {}),
        pytest.raises(KeyError),
    ):
        get_instrument_settings("instrument1", "key2")


def test_get_path_returns_setting_for_existing_instrument_and_key() -> None:
    """Test that correct path is returned."""
    with (
        patch(
            "common.settings._SETTINGS",
            {"locations": {"backup": {"absolute_path": "some_path"}}},
        ),
    ):
        assert get_path("backup") == Path("some_path")


def test_get_path_returns_setting_raises_key_error_for_non_exisiting_key_1() -> None:
    """Test that a KeyError is raised if the key does not exist."""
    with (
        patch(
            "common.settings._SETTINGS",
            {"locations": {"backup": {"absolute_path": "some_path"}}},
        ),
        pytest.raises(KeyError),
    ):
        get_path("Xbackup")


def test_get_path_returns_setting_raises_key_error_for_non_exisiting_key_2() -> None:
    """Test that a KeyError is raised if the key does not exist."""
    with (
        patch(
            "common.settings._SETTINGS",
            {"locations": {"backup": {"Xabsolute_path": "some_path"}}},
        ),
        pytest.raises(KeyError),
    ):
        get_path("backup")


def test_get_instrument_settings_raises_key_error_for_non_existing_instrument() -> None:
    """Test that a KeyError is raised if the instrument does not exist in the instrument settings."""
    with (
        patch("common.settings._INSTRUMENTS", {"instrument1": {"key1": "value1"}}),
        patch("common.settings.INSTRUMENT_SETTINGS_DEFAULTS", {}),
        pytest.raises(KeyError),
    ):
        get_instrument_settings("instrument2", "key1")


def test_get_instrument_settings_returns_default_setting_if_key_not_in_instrument_settings() -> (
    None
):
    """Test that a default setting is returned if the key is not in the instrument settings."""
    with (
        patch("common.settings._INSTRUMENTS", {"instrument1": {}}),
        patch(
            "common.settings.INSTRUMENT_SETTINGS_DEFAULTS", {"key1": "default_value"}
        ),
    ):
        assert get_instrument_settings("instrument1", "key1") == "default_value"
