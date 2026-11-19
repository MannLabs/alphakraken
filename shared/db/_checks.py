"""Checks shared by the write functions of `shared.db`."""


def inactive_errors(label: str, status: str, active_status: str) -> list[str]:
    """Return an error if a record that a write refers to is not active.

    :param label: the record as named in the message, e.g. `Project 'P1'`
    :param active_status: the active status of its kind, e.g. `ProjectStatus.ACTIVE`
    """
    if status == active_status:
        return []
    return [f"{label} is not active (status '{status}')."]
