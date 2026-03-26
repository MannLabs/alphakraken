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
        "locations": {
            "settings": {"absolute_path": "./tmp/test/settings"},
            "output": {"absolute_path": "./tmp/test/output"},
            "backup": {"absolute_path": "./tmp/test/backup"},
            "slurm": {"absolute_path": "./tmp/test/slurm"},
            "software": {"absolute_path": "./tmp/test/software"},
        },
    }


class TestGetPurgingVerificationType:
    """Tests for get_purging_verification_type cross-validation."""

    @patch(
        "shared.yamlsettings.YAMLSETTINGS",
        {"backup": {"backup_type": "s3", "purging_verification_type": "local"}},
    )
    def test_raises_on_local_with_s3_backup(self) -> None:
        """Test that local purging verification with s3 backup raises ValueError."""
        from shared.yamlsettings import get_purging_verification_type

        with pytest.raises(
            ValueError, match="purging_verification_type='local' is not allowed"
        ):
            get_purging_verification_type()

    @patch(
        "shared.yamlsettings.YAMLSETTINGS",
        {"backup": {"backup_type": "s3", "purging_verification_type": "force_local"}},
    )
    def test_force_local_with_s3_backup(self) -> None:
        """Test that force_local is allowed with s3 backup."""
        from shared.yamlsettings import get_purging_verification_type

        assert get_purging_verification_type() == "force_local"

    @patch(
        "shared.yamlsettings.YAMLSETTINGS",
        {"backup": {"backup_type": "s3", "purging_verification_type": "s3"}},
    )
    def test_s3_with_s3_backup(self) -> None:
        """Test that s3 purging verification with s3 backup is allowed."""
        from shared.yamlsettings import get_purging_verification_type

        assert get_purging_verification_type() == "s3"
