"""Resolve direct message recipients from the initials in raw file names."""

import re

from .config import INSTRUMENT_USER_SLACK_IDS, SPECIAL_ALERT_SLACK_ID

# Trailing boundary is a lookahead so adjacent tokens can both match:
# "_SA_AnSc_" must yield both "SA" and "AnSc", not just "SA".
INITIALS_PATTERN = re.compile(r"_([A-Za-z]{2,8})(?=_)")


def extract_initials(name: str | None) -> str | None:
    """Return the first regex match whose token is a mapped initial, else None."""
    if not name:
        return None
    for match in INITIALS_PATTERN.finditer(name):
        token = match.group(1)
        if token in INSTRUMENT_USER_SLACK_IDS:
            return token
    return None


def get_recipients(file_name: str | None) -> list[str]:
    """Return the deduplicated recipients for a file: its mapped user plus SPECIAL_ALERT_SLACK_ID."""
    initials = extract_initials(file_name)
    if initials is None:
        return []

    recipients = [INSTRUMENT_USER_SLACK_IDS[initials]]
    if SPECIAL_ALERT_SLACK_ID and SPECIAL_ALERT_SLACK_ID not in recipients:
        recipients.append(SPECIAL_ALERT_SLACK_ID)
    return recipients
