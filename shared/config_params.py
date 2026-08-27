"""Placeholders that can be used in the `config_params` of a settings entry."""

import re

from shared.keys import ConstantsClass

# stand-in for a placeholder value, used where the real values are not known yet
PLACEHOLDER_DUMMY_VALUE = "dummy"

UNKNOWN_PLACEHOLDER_ERROR = "Unknown placeholder"


class ConfigParamPlaceholders(metaclass=ConstantsClass):
    """Names of the placeholders, without the surrounding braces."""

    PROJECT_ID: str = "PROJECT_ID"
    RAW_FILE_ID: str = "RAW_FILE_ID"
    RAW_FILE_PATH: str = "RAW_FILE_PATH"
    RELATIVE_RAW_FILE_PATH: str = "RELATIVE_RAW_FILE_PATH"
    SETTINGS_PATH: str = "SETTINGS_PATH"
    OUTPUT_PATH: str = "OUTPUT_PATH"
    RELATIVE_OUTPUT_PATH: str = "RELATIVE_OUTPUT_PATH"
    NUM_THREADS: str = "NUM_THREADS"


PLACEHOLDER_DESCRIPTIONS: dict[str, str] = {
    ConfigParamPlaceholders.PROJECT_ID: "project id",
    ConfigParamPlaceholders.RAW_FILE_ID: "name of the raw file",
    ConfigParamPlaceholders.RAW_FILE_PATH: "absolute path of the raw file",
    ConfigParamPlaceholders.RELATIVE_RAW_FILE_PATH: "path of the raw file relative to `locations.backup.absolute_path` in alphakraken.yaml",
    ConfigParamPlaceholders.SETTINGS_PATH: "absolute path of the settings directory",
    ConfigParamPlaceholders.OUTPUT_PATH: "absolute path of the output directory",
    ConfigParamPlaceholders.RELATIVE_OUTPUT_PATH: "path of the output directory relative to `locations.output.absolute_path` in alphakraken.yaml",
    ConfigParamPlaceholders.NUM_THREADS: "number of threads",
}


_KNOWN_PLACEHOLDER_PATTERN = re.compile(
    rf"\{{({'|'.join(ConfigParamPlaceholders.get_values())})\}}"
)
_BRACED_TOKEN_PATTERN = re.compile(r"\{([^{}]*)\}")


def substitute_placeholders(config_params: str, values: dict[str, str]) -> str:
    """Replace each `{PLACEHOLDER}` in `config_params` by the given value."""
    # single pass, so that a substituted value containing a placeholder is not expanded again
    return _KNOWN_PLACEHOLDER_PATTERN.sub(
        lambda match: values.get(match.group(1), match.group()), config_params
    )


def substitute_dummy_values(config_params: str) -> str:
    """Replace all known placeholders by a dummy value, to enable validating unresolved `config_params`."""
    return substitute_placeholders(
        config_params,
        dict.fromkeys(ConfigParamPlaceholders.get_values(), PLACEHOLDER_DUMMY_VALUE),
    )


def check_for_unknown_placeholders(config_params: str) -> list[str]:
    """Validate that `config_params` contains no braced token besides the known placeholders.

    Returns:
        list[str]: List of validation error messages (empty if valid)

    """
    known_placeholders = ConfigParamPlaceholders.get_values()
    return [
        f"{UNKNOWN_PLACEHOLDER_ERROR}: {match.group()}"
        for match in _BRACED_TOKEN_PATTERN.finditer(config_params)
        if match.group(1) not in known_placeholders
    ]
