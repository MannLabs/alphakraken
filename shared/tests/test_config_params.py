"""Unit tests for the config_params placeholder substitution."""

from shared.config_params import (
    PLACEHOLDER_DESCRIPTIONS,
    PLACEHOLDER_DUMMY_VALUE,
    ConfigParamPlaceholders,
    check_for_unknown_placeholders,
    substitute_dummy_values,
    substitute_placeholders,
)
from shared.validation import check_for_malicious_content


def test_all_placeholders_have_a_description() -> None:
    """Test that the webapp help text covers every placeholder."""
    assert set(PLACEHOLDER_DESCRIPTIONS) == set(ConfigParamPlaceholders.get_values())


def test_substitute_placeholders() -> None:
    """Test that braced placeholders are replaced."""
    result = substitute_placeholders(
        "--f {RAW_FILE_PATH} --threads {NUM_THREADS}",
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

    result = substitute_placeholders("{RAW_FILE_PATH} {RELATIVE_RAW_FILE_PATH}", values)
    result_reversed = substitute_placeholders(
        "{RAW_FILE_PATH} {RELATIVE_RAW_FILE_PATH}", dict(reversed(values.items()))
    )

    assert result == "/backup/f.raw instrument1/f.raw"
    assert result_reversed == result


def test_substitute_placeholders_does_not_expand_substituted_values() -> None:
    """Test that a placeholder contained in a substituted value is not expanded again."""
    result = substitute_placeholders(
        "{OUTPUT_PATH} {NUM_THREADS}",
        {
            ConfigParamPlaceholders.OUTPUT_PATH: "/out/{NUM_THREADS}",
            ConfigParamPlaceholders.NUM_THREADS: "8",
        },
    )

    assert result == "/out/{NUM_THREADS} 8"


def test_substitute_dummy_values() -> None:
    """Test that all known placeholders are replaced by the dummy value."""
    config_params = " ".join(
        f"{{{placeholder}}}" for placeholder in ConfigParamPlaceholders.get_values()
    )

    result = substitute_dummy_values(config_params)

    assert result == " ".join(
        [PLACEHOLDER_DUMMY_VALUE] * len(ConfigParamPlaceholders.get_values())
    )


def test_dummy_substituted_params_pass_validation() -> None:
    """Test that config params using placeholders are accepted by the validation."""
    config_params = "--f {RAW_FILE_PATH} --lib {SETTINGS_PATH}/library.speclib --threads {NUM_THREADS}"

    errors = check_for_malicious_content(
        substitute_dummy_values(config_params), allow_spaces=True
    )

    assert errors == []


def test_check_for_unknown_placeholders_accepts_known_ones() -> None:
    """Test that all known placeholders pass the check."""
    config_params = " ".join(
        f"{{{placeholder}}}" for placeholder in ConfigParamPlaceholders.get_values()
    )

    assert check_for_unknown_placeholders(config_params) == []


def test_check_for_unknown_placeholders_rejects_misspelled_one() -> None:
    """Test that a misspelled placeholder is reported by name."""
    errors = check_for_unknown_placeholders(
        "--f {RAW_FILE_PAHT} --threads {NUM_THREADS}"
    )

    assert len(errors) == 1
    assert "{RAW_FILE_PAHT}" in errors[0]
