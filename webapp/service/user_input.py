"""Normalization and parsing of user input on the admin page."""

import re

# comma, or tab as in rows pasted from a spreadsheet
USER_ROW_SEPARATOR_REGEX = r"[,\t]"


def clean_initials(initials: str) -> str:
    """Normalize initials."""
    return initials.strip()


def clean_messenger_id(messenger_id: str) -> str | None:
    """Normalize a messenger id, None if empty."""
    return messenger_id.strip() or None


def parse_user_row(row: str) -> tuple[str, str, str | None]:
    """Parse a row of `email,initials[,messenger id]` (or tab-separated) into an (email, initials, messenger id) tuple."""
    fields = re.split(USER_ROW_SEPARATOR_REGEX, row)
    if len(fields) not in (2, 3):
        raise ValueError(f"expected 'e-mail,initials[,messenger id]', got '{row}'.")
    messenger_id = clean_messenger_id(fields[2]) if len(fields) == 3 else None  # noqa: PLR2004
    return fields[0], clean_initials(fields[1]), messenger_id
