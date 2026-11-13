"""Checks shared by the write functions of `shared.db`."""

from shared.domain.errors import ValidationError


def check_status_transition(
    transitions: dict[str, set[str]], label: str, current: str, new: str
) -> None:
    """Raise unless the status of the record may change from `current` to `new`.

    :param transitions: the statuses that may follow each status
    :param label: the record as named in the message, e.g. `project 'P1'`

    Raises:
        ValidationError: if the transition is not allowed.

    """
    if new not in transitions.get(current, ()):
        raise ValidationError([f"Cannot set {label} from '{current}' to '{new}'."])
