"""Tests for the settings module."""

from pathlib import Path
from unittest.mock import patch

import pytest
from common.settings import get_instrument_settings

from shared.yamlsettings import get_path


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
            "shared.yamlsettings.YAMLSETTINGS",
            {"locations": {"backup": {"absolute_path": "some_path"}}},
        ),
    ):
        assert get_path("backup") == Path("some_path")


def test_get_path_returns_setting_raises_key_error_for_non_exisiting_key_1() -> None:
    """Test that a KeyError is raised if the key does not exist."""
    with (
        patch(
            "shared.yamlsettings.YAMLSETTINGS",
            {"locations": {"backup": {"absolute_path": "some_path"}}},
        ),
        pytest.raises(KeyError),
    ):
        get_path("Xbackup")


def test_get_path_returns_setting_raises_key_error_for_non_exisiting_key_2() -> None:
    """Test that a KeyError is raised if the key does not exist."""
    with (
        patch(
            "shared.yamlsettings.YAMLSETTINGS",
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
