"""Resolve direct message recipients from the initials of users in raw file names."""

import re
from collections.abc import Collection

from shared.db.interface import get_all_users

# Trailing boundary is a lookahead so adjacent tokens can both match:
# "_SA_AnSc_" must yield both "SA" and "AnSc", not just "SA".
INITIALS_PATTERN = re.compile(r"_([A-Za-z]{2,8})(?=_)")


def get_messenger_ids_by_initials() -> dict[str, str]:
    """Return the messenger ids of all active users that have one, by initials."""
    return {
        user.initials: user.messenger_id
        for user in get_all_users()
        if user.messenger_id
    }


def extract_initials(name: str | None, known_initials: Collection[str]) -> str | None:
    """Return the first token of the name that is one of the known initials, else None."""
    if not name:
        return None
    for match in INITIALS_PATTERN.finditer(name):
        token = match.group(1)
        if token in known_initials:
            return token
    return None


def get_recipients(
    file_name: str | None, messenger_ids_by_initials: dict[str, str]
) -> list[str]:
    """Return the messenger id of the user whose initials are in the file name, if any."""
    initials = extract_initials(file_name, messenger_ids_by_initials)
    return [] if initials is None else [messenger_ids_by_initials[initials]]
