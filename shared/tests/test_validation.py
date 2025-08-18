"""Unit tests for shared validation functions."""

from shared.validation import (
    EXECUTABLE_ABSOLUTE_PATH_ERROR,
    EXECUTABLE_EMPTY_ERROR,
    EXECUTABLE_INVALID_CHARS_ERROR,
    EXECUTABLE_PARENT_DIR_ERROR,
    validate_name,
)


class TestValidateName:
    """Test cases for validate_name function."""

    def test_valid_executable_names(self) -> None:
        """Test that valid executable names pass validation."""
        valid_names_no_spaces = [
            "valid-executable",
            "executable.1.2.3",
            "sub/folder/executable",
            "deep/nested/path/executable",
            "exe_with_underscores",
            "exe-with-hyphens",
            "exe.with.dots",
            "123numeric",
            "a",
            "a/b",
        ]

        for name in valid_names_no_spaces:
            errors = validate_name(name)
            assert not errors, f"Expected '{name}' to be valid, got errors: {errors}"

    def test_valid_executable_names_with_spaces(self) -> None:
        """Test that valid executable names with spaces pass validation when allow_spaces=True."""
        valid_names_with_spaces = [
            "exe with spaces",
        ]

        for name in valid_names_with_spaces:
            errors = validate_name(name, allow_spaces=True)
            assert not errors, (
                f"Expected '{name}' to be valid with spaces allowed, got errors: {errors}"
            )

    def test_empty_executable_name(self) -> None:
        """Test that empty executable name fails validation."""
        errors = validate_name("")
        assert errors == [EXECUTABLE_EMPTY_ERROR]

    def test_parent_directory_references(self) -> None:
        """Test that parent directory references fail validation."""
        invalid_names = [
            "../executable",
            "folder/../executable",
            "../../../bin/bash",
            "exe..cutable",
            "..executable",
            "executable..",
        ]

        for name in invalid_names:
            errors = validate_name(name)
            assert errors, f"Expected '{name}' to be invalid"
            assert EXECUTABLE_PARENT_DIR_ERROR in errors

    def test_absolute_paths(self) -> None:
        """Test that absolute paths fail validation."""
        invalid_names = [
            "/usr/bin/bash",
            "/bin/sh",
            "/executable",
            "/path/to/executable",
        ]

        for name in invalid_names:
            errors = validate_name(name)
            assert errors, f"Expected '{name}' to be invalid"
            assert EXECUTABLE_ABSOLUTE_PATH_ERROR in errors

    def test_invalid_characters(self) -> None:
        """Test that invalid characters fail validation."""
        invalid_names = [
            "exe;rm",
            "exe&malicious",
            "exe|cmd",
            "exe$var",
            "exe`cmd`",
            "exe(cmd)",
            "exe<file",
            "exe>file",
            "exe*glob",
            "exe?query",
            "exe[array]",
            "exe{block}",
            "exe~home",
            "exe!bang",
            "exe\\backslash",
            'exe"quote',
            "exe'quote",
            "exe\ttab",
            "exe\nnewline",
        ]

        for name in invalid_names:
            errors = validate_name(name)
            assert errors, f"Expected '{name}' to be invalid"
            assert EXECUTABLE_INVALID_CHARS_ERROR in errors
