"""Unit tests for shared validation functions."""

from shared.validation import (
    ABSOLUTE_PATH_ERROR,
    INVALID_CHARS_ERROR,
    PARENT_DIR_ERROR,
    TRAILING_BACKSLASH_ERROR,
    check_for_malicious_content,
)


class TestValidateName:
    """Test cases for validate_name function."""

    def test_check_for_malicious_content_valid_executable_names(self) -> None:
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
            "sub\\folder\\executable.cmd",
        ]

        for name in valid_names_no_spaces:
            errors = check_for_malicious_content(name)
            assert not errors, f"Expected '{name}' to be valid, got errors: {errors}"

    def test_check_for_malicious_content_valid_executable_names_with_spaces(
        self,
    ) -> None:
        """Test that valid executable names with spaces pass validation when allow_spaces=True."""
        valid_names_with_spaces = [
            "exe with spaces",
            "direct -r dummy -s dummy\\settings.prop -o dummy",
        ]

        for name in valid_names_with_spaces:
            errors = check_for_malicious_content(name, allow_spaces=True)
            assert not errors, (
                f"Expected '{name}' to be valid with spaces allowed, got errors: {errors}"
            )

    def test_check_for_malicious_content_parent_directory_references(self) -> None:
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
            errors = check_for_malicious_content(name)
            assert errors, f"Expected '{name}' to be invalid"
            assert PARENT_DIR_ERROR in errors

    def test_check_for_malicious_content_absolute_paths(self) -> None:
        """Test that absolute paths fail validation."""
        invalid_names = [
            "/usr/bin/bash",
            "/bin/sh",
            "/executable",
            "/path/to/executable",
            "\\Windows\\System32\\cmd.exe",
            "\\\\host\\share\\executable.exe",
        ]

        for name in invalid_names:
            errors = check_for_malicious_content(name)
            assert errors, f"Expected '{name}' to be invalid"
            assert ABSOLUTE_PATH_ERROR in errors

    def test_check_for_malicious_content_invalid_characters(self) -> None:
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
            "exe:colon",
            'exe"quote',
            "exe'quote",
            "exe\ttab",
            "exe\nnewline",
        ]

        for name in invalid_names:
            errors = check_for_malicious_content(name)
            assert errors, f"Expected '{name}' to be invalid"
            assert INVALID_CHARS_ERROR in errors

    def test_check_for_malicious_content_trailing_backslash(self) -> None:
        """Test that a trailing backslash fails validation, also with spaces allowed."""
        for allow_spaces in (False, True):
            errors = check_for_malicious_content("exe\\", allow_spaces=allow_spaces)
            assert errors == [TRAILING_BACKSLASH_ERROR]
