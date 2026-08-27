"""Placeholders that can be used in the `config_params` of a settings entry."""

from shared.keys import ConstantsClass

# stand-in for a placeholder value, used where the real values are not known yet
PLACEHOLDER_DUMMY_VALUE = "dummy"


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


def substitute_placeholders(config_params: str, values: dict[str, str]) -> str:
    """Replace each `{PLACEHOLDER}` in `config_params` by the given value."""
    for placeholder, value in values.items():
        config_params = config_params.replace(f"{{{placeholder}}}", value)
    return config_params


def substitute_dummy_values(config_params: str) -> str:
    """Replace all known placeholders by a dummy value, to enable validating unresolved `config_params`."""
    return substitute_placeholders(
        config_params,
        dict.fromkeys(ConfigParamPlaceholders.get_values(), PLACEHOLDER_DUMMY_VALUE),
    )
