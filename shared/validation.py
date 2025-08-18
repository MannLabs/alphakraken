"""Shared validation functions for security and input validation."""

import re

# Security validation constants
EXECUTABLE_NAME_PATTERN = r"^[a-zA-Z0-9._/-]+$"

# fmt: off
SHELL_METACHARACTERS = {    ";", "&", "|", "$", "`", "(", ")", "<", ">", "*", "?", "[", "]", "{", "}", "~", "!", "\\", '"', "'" }
# fmt: on

# Error messages
EXECUTABLE_EMPTY_ERROR = "Cannot be empty"
EXECUTABLE_PARENT_DIR_ERROR = "Cannot contain '..'"
EXECUTABLE_ABSOLUTE_PATH_ERROR = "Cannot be an absolute path"
EXECUTABLE_INVALID_CHARS_ERROR = "Contains invalid characters. Only letters, numbers, dots, hyphens, underscores, and forward slashes are allowed"


def validate_name(executable: str) -> list[str]:
    """Validate name for security (prevent path traversal).

    Returns:
        tuple[bool, str]: (is_valid, error_message)

    """
    errors = []
    if not executable:
        return [EXECUTABLE_EMPTY_ERROR]

    # Check for parent directory references
    if ".." in executable:
        errors.append(EXECUTABLE_PARENT_DIR_ERROR)

    # Check for absolute paths
    if executable.startswith("/"):
        errors.append(EXECUTABLE_ABSOLUTE_PATH_ERROR)

    # Validate allowed characters
    if not re.match(EXECUTABLE_NAME_PATTERN, executable):
        errors.append(EXECUTABLE_INVALID_CHARS_ERROR)

    return errors


def validate_config_params(config_params: str) -> list[str]:
    """Validate config parameters for security (prevent shell injection).

    Returns:
        tuple[bool, str]: (is_valid, error_message)

    """
    errors = []
    if not config_params:
        return errors  # Empty params are allowed

    # Check for shell metacharacters
    found_forbidden = []
    for char in config_params:
        if char in SHELL_METACHARACTERS:
            found_forbidden.append(char)  # noqa: PERF401

    if found_forbidden:
        unique_forbidden = sorted(set(found_forbidden))
        error_msg = f"Config parameters contain forbidden shell metacharacters: {', '.join(unique_forbidden)} "
        errors.append(error_msg)

    return errors
