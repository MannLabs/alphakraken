"""Normalization and parsing of user input on the admin page."""

USER_ROW_SEPARATOR = ","


def clean_email(email: str) -> str:
    """Normalize an email."""
    return email.strip().lower()


def clean_initials(initials: str) -> str:
    """Normalize initials."""
    return initials.strip().upper()


def parse_user_row(row: str) -> tuple[str, str]:
    """Parse a row of `email,initials` into a normalized (email, initials) tuple."""
    fields = row.split(USER_ROW_SEPARATOR)
    if len(fields) != 2:  # noqa: PLR2004
        raise ValueError(f"expected 'e-mail{USER_ROW_SEPARATOR}initials', got '{row}'.")
    return clean_email(fields[0]), clean_initials(fields[1])
