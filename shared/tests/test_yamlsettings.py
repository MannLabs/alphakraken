"""Tests for the YamlSettings class."""

import os
from collections.abc import Generator
from unittest.mock import MagicMock, mock_open, patch

import pytest


@pytest.fixture(autouse=True)
def mock_env_for_import() -> Generator[None, None, None]:
    """Set ENV_NAME to test before any imports to prevent initialization issues."""
    with patch.dict(os.environ, {"ENV_NAME": "_test_"}):
        yield


@patch("shared.yamlsettings.os.getenv")
@patch("shared.yamlsettings.Path")
def test_loads_alphakraken_yaml_successfully(
    mock_path: MagicMock,
    mock_get_env_variable: MagicMock,
) -> None:
    """Test that the settings are loaded successfully from a YAML file."""
    # Clear any existing instance to ensure fresh test
    from shared.yamlsettings import YamlSettings

    mock_get_env_variable.return_value = "sandbox"

    yaml_content = """
    instruments:
      instrument1:
        type: thermo
        skip_quanting: true
        min_free_space_gb: 123
    """

    mock_path.return_value.__truediv__.return_value.exists.return_value = True
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


@patch("shared.yamlsettings.os.getenv")
@patch("shared.yamlsettings.Path")
def test_raises_file_not_found_error_if_file_does_not_exist(
    mock_path: MagicMock,
    mock_get_env_variable: MagicMock,
) -> None:
    """Test that a FileNotFoundError is raised if the settings file does not exist."""
    from shared.yamlsettings import YamlSettings

    mock_get_env_variable.return_value = "sandbox"
    mock_path.return_value.__truediv__.return_value.exists.return_value = False

    with pytest.raises(FileNotFoundError):
        YamlSettings.load_alphakraken_yaml()


@patch("shared.yamlsettings.os.getenv")
def test_returns_test_settings_for_test_environment(
    mock_get_env_variable: MagicMock,
) -> None:
    """Test that test settings are returned when the environment is set to test."""
    from shared.yamlsettings import YamlSettings

    mock_get_env_variable.return_value = "_test_"

    # when
    settings = YamlSettings.load_alphakraken_yaml()

    assert settings == {
        "instruments": {"_test1_": {"type": "thermo"}},
        "general": {
            "notifications": {
                "ops_alerts_webhook_url": "http://test-webhook.example.com",
                "business_alerts_webhook_url": "http://test-webhook.example.com",
                "hostname": "localhost",
                "webapp_url": "http://localhost:8501",
            }
        },
    }
