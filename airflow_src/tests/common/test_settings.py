"""Tests for the settings module."""

from unittest.mock import MagicMock, mock_open, patch

import pytest
from common.settings import _load_alphakraken_yaml


@patch("common.settings.Path")
def test_loads_alphakraken_yaml_successfully(mock_path: MagicMock) -> None:
    """Test that the settings are loaded successfully from a YAML file."""
    env_name = "production"

    yaml_content = """
    instruments:
      instrument1:
        type: thermo
    """

    mock_path.return_value.__truediv__.return_value.open = mock_open(
        read_data=yaml_content
    )

    settings = _load_alphakraken_yaml(env_name)
    assert settings == {"instruments": {"instrument1": {"type": "thermo"}}}


@patch("common.settings.Path")
def test_raises_file_not_found_error_if_file_does_not_exist(
    mock_path: MagicMock,
) -> None:
    """Test that a FileNotFoundError is raised if the settings file does not exist."""
    env_name = "production"
    mock_path.return_value.__truediv__.return_value.exists.return_value = False
    with pytest.raises(FileNotFoundError):
        _load_alphakraken_yaml(env_name)


def test_returns_test_settings_for_test_environment() -> None:
    """Test that test settings are returned when the environment is set to test."""
    env_name = "_test_"
    settings = _load_alphakraken_yaml(env_name)
    assert settings == {"instruments": {"_test1_": {"type": "thermo"}}}
