"""Parsing of user input on the admin page."""

import re

# comma, or tab as in rows pasted from a spreadsheet
USER_ROW_SEPARATOR_REGEX = r"[,\t]"


def parse_user_row(row: str) -> tuple[str, str, str | None]:
    """Parse a row of `email,initials[,messenger id]` (or tab-separated) into an (email, initials, messenger id) tuple."""
    fields = re.split(USER_ROW_SEPARATOR_REGEX, row)
    if len(fields) not in (2, 3):
        raise ValueError(f"expected 'e-mail,initials[,messenger id]', got '{row}'.")
    messenger_id = fields[2] if len(fields) == 3 else None  # noqa: PLR2004
    return fields[0], fields[1], messenger_id
