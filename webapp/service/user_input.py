"""Normalization and parsing of user input on the admin page."""

USER_ROW_SEPARATOR = ","


def clean_email(email: str) -> str:
    """Normalize an email."""
    return email.strip().lower()


def clean_initials(initials: str) -> str:
    """Normalize initials."""
    return initials.strip()


def clean_messenger_id(messenger_id: str) -> str | None:
    """Normalize a messenger id, None if empty."""
    return messenger_id.strip() or None


def parse_user_row(row: str) -> tuple[str, str, str | None]:
    """Parse a row of `email,initials[,messenger_id]` into a normalized (email, initials, messenger_id) tuple."""
    fields = row.split(USER_ROW_SEPARATOR)
    if len(fields) not in (2, 3):
        raise ValueError(
            f"expected 'e-mail{USER_ROW_SEPARATOR}initials[{USER_ROW_SEPARATOR}messenger id]', got '{row}'."
        )
    messenger_id = clean_messenger_id(fields[2]) if len(fields) == 3 else None  # noqa: PLR2004
    return clean_email(fields[0]), clean_initials(fields[1]), messenger_id
