"""Utility functions for monitoring and error handling."""

import traceback


def extract_error_line(exception: Exception, line: int = 4) -> str:
    """Extract a line from a traceback.

    This provides the actual line of code that caused the error,
    which is more informative than just the exception message.
    """
    tb_lines = traceback.format_exception(
        type(exception), exception, exception.__traceback__
    )

    full_traceback = "".join(tb_lines).split("\n")

    non_empty_lines = [line for line in full_traceback if line.strip()]

    if len(non_empty_lines) >= line:
        return non_empty_lines[-line].strip()

    return "n/a"
