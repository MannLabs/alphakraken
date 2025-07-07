"""Tests for the YamlSettings class."""

from unittest.mock import MagicMock, mock_open, patch

import pytest

from shared.yamlsettings import YamlSettings


@patch("os.environ")
@patch("shared.yamlsettings.Path")
def test_loads_alphakraken_yaml_successfully(
    mock_path: MagicMock,
    mock_get_env_variable: MagicMock,
) -> None:
    """Test that the settings are loaded successfully from a YAML file."""
    mock_get_env_variable.return_value = "sandbox"

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
    settings = YamlSettings.load_alphakraken_yaml()

    assert settings == {
        "instruments": {
            "instrument1": {
                "type": "thermo",
                "skip_quanting": True,
                "min_free_space_gb": 123,
            }
        }
    }


@patch("os.environ")
@patch("shared.yamlsettings.Path")
def test_raises_file_not_found_error_if_file_does_not_exist(
    mock_path: MagicMock,
    mock_get_env_variable: MagicMock,
) -> None:
    """Test that a FileNotFoundError is raised if the settings file does not exist."""
    mock_get_env_variable.return_value = "sandbox"
    # when
    mock_path.return_value.__truediv__.return_value.exists.return_value = False

    with pytest.raises(FileNotFoundError):
        YamlSettings.load_alphakraken_yaml()


def test_returns_test_settings_for_test_environment() -> None:
    """Test that test settings are returned when the environment is set to test."""
    # when
    settings = YamlSettings.load_alphakraken_yaml()

    assert settings == {"instruments": {"_test1_": {"type": "thermo"}}}
