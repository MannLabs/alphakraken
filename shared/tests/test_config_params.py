"""Unit tests for the config_params placeholder substitution."""

import pytest

from shared.config_params import (
    PLACEHOLDER_DESCRIPTIONS,
    ConfigParamPlaceholders,
    check_config_params,
    substitute_placeholders,
)
from shared.validation import INVALID_CHARS_ERROR_WITH_SPACES


def test_all_placeholders_have_a_description() -> None:
    """Test that the webapp help text covers every placeholder."""
    assert set(PLACEHOLDER_DESCRIPTIONS) == set(ConfigParamPlaceholders.get_values())


def test_substitute_placeholders() -> None:
    """Test that braced placeholders are replaced."""
    result = substitute_placeholders(
        "--f {{RAW_FILE_PATH}} --threads {{NUM_THREADS}}",
        {
            ConfigParamPlaceholders.RAW_FILE_PATH: "/backup/f.raw",
            ConfigParamPlaceholders.NUM_THREADS: "8",
        },
    )

    assert result == "--f /backup/f.raw --threads 8"


def test_substitute_placeholders_ignores_unbraced_names() -> None:
    """Test that a bare placeholder name is left untouched."""
    result = substitute_placeholders(
        "--f RAW_FILE_PATH",
        {ConfigParamPlaceholders.RAW_FILE_PATH: "/backup/f.raw"},
    )

    assert result == "--f RAW_FILE_PATH"


def test_substitute_placeholders_is_order_independent() -> None:
    """Test that a placeholder that is the suffix of another one is not substituted into it."""
    values = {
        ConfigParamPlaceholders.RAW_FILE_PATH: "/backup/f.raw",
        ConfigParamPlaceholders.RELATIVE_RAW_FILE_PATH: "instrument1/f.raw",
    }

    result = substitute_placeholders(
        "{{RAW_FILE_PATH}} {{RELATIVE_RAW_FILE_PATH}}", values
    )
    result_reversed = substitute_placeholders(
        "{{RAW_FILE_PATH}} {{RELATIVE_RAW_FILE_PATH}}", dict(reversed(values.items()))
    )

    assert result == "/backup/f.raw instrument1/f.raw"
    assert result_reversed == result


def test_substitute_placeholders_does_not_expand_substituted_values() -> None:
    """Test that a placeholder contained in a substituted value is not expanded again."""
    result = substitute_placeholders(
        "{{OUTPUT_PATH}} {{NUM_THREADS}}",
        {
            ConfigParamPlaceholders.OUTPUT_PATH: "/out/{{NUM_THREADS}}",
            ConfigParamPlaceholders.NUM_THREADS: "8",
        },
    )

    assert result == "/out/{{NUM_THREADS}} 8"


def test_check_config_params_accepts_all_known_placeholders() -> None:
    """Test that all known placeholders pass the check."""
    config_params = " ".join(
        f"{{{{{placeholder}}}}}" for placeholder in ConfigParamPlaceholders.get_values()
    )

    assert check_config_params(config_params) == []


@pytest.mark.parametrize(
    ("config_params", "errors"),
    [
        ("--f {{RAW_FILE_PATH}} --threads {{NUM_THREADS}}", []),
        # reported once, not again for its braces
        (
            "--f {{RAW_FILE_PAHT}} --threads {{NUM_THREADS}}",
            ["Unknown placeholder: {{RAW_FILE_PAHT}}"],
        ),
        # the pre-double-brace syntax is not passed through unsubstituted
        (
            "--f {RAW_FILE_PATH} --out {{OUTPUT_PATH}}",
            ["Unknown placeholder: {RAW_FILE_PATH}"],
        ),
        ("--f {{RAW_FILE_PATH}}; rm -rf /", [INVALID_CHARS_ERROR_WITH_SPACES]),
    ],
)
def test_check_config_params(config_params: str, errors: list[str]) -> None:
    """Known placeholders pass, an unknown one or malicious content around them does not."""
    assert check_config_params(config_params) == errors
