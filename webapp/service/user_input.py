"""Normalization and parsing of user input on the admin page."""

import re

# comma, or tab as in rows pasted from a spreadsheet
USER_ROW_SEPARATOR_REGEX = r"[,\t]"


def clean_initials(initials: str) -> str:
    """Normalize initials."""
    return initials.strip()


def parse_user_row(row: str) -> tuple[str, str]:
    """Parse a row of `email,initials` (or tab-separated) into an (email, initials) tuple."""
    fields = re.split(USER_ROW_SEPARATOR_REGEX, row)
    if len(fields) != 2:  # noqa: PLR2004
        raise ValueError(f"expected 'e-mail,initials', got '{row}'.")
    return fields[0], clean_initials(fields[1])
