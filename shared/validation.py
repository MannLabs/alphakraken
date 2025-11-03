"""Shared validation functions for security and input validation."""

import re

from shared.keys import _ALLOWED_CHARACTERS

# Security validation constants
# Base pattern allows filenames with safe characters, path pattern adds forward slash
EXECUTABLE_NAME_PATTERN = rf"^[{_ALLOWED_CHARACTERS}/]+$"
EXECUTABLE_NAME_PATTERN_WITH_SPACES = rf"^[{_ALLOWED_CHARACTERS}/ ]+$"


# Error messages
EMPTY_ERROR = "Cannot be empty"
PARENT_DIR_ERROR = "Cannot contain '..'"
ABSOLUTE_PATH_ERROR = "Cannot be an absolute path"
# Error messages derive from _ALLOWED_CHARACTERS to maintain single source of truth
INVALID_CHARS_ERROR = "Contains invalid characters. Only alphanumeric characters, dots, hyphens, underscores, plus signs, and forward slashes are allowed"
INVALID_CHARS_ERROR_WITH_SPACES = "Contains invalid characters. Only alphanumeric characters, dots, hyphens, underscores, plus signs, forward slashes, and spaces are allowed"


def check_for_malicious_content(
    value: str, *, allow_spaces: bool = False, allow_absolute_paths: bool = False
) -> list[str]:
    """Validate a value for security (prevent command injection).

    Args:
        value: The value to validate
        allow_spaces: Whether to allow spaces in the name (default: False)
        allow_absolute_paths: Whether to allow absolute paths (default: False)

    Returns:
        list[str]: List of validation error messages (empty if valid)

    """
    errors = []
    if not value:
        return errors

    # Check for parent directory references
    if ".." in value:
        errors.append(PARENT_DIR_ERROR)

    # Check for absolute paths
    if value.startswith("/") and not allow_absolute_paths:
        errors.append(ABSOLUTE_PATH_ERROR)

    # Validate allowed characters
    if allow_spaces:
        pattern = EXECUTABLE_NAME_PATTERN_WITH_SPACES
        error_msg = INVALID_CHARS_ERROR_WITH_SPACES
    else:
        pattern = EXECUTABLE_NAME_PATTERN
        error_msg = INVALID_CHARS_ERROR

    if not re.match(pattern, value):
        errors.append(error_msg)

    return errors
