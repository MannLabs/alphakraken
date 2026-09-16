"""Custom Airflow exceptions."""

from airflow.exceptions import AirflowFailException, AirflowSkipException

# TODO: move all custom exceptions here


class QuantingFailedNewErrorException(AirflowFailException):
    """Raise if quanting failed with a new error."""


class QuantingFailedKnownErrorException(AirflowSkipException):
    """Raise if quanting failed with a known error."""


class QuantingFailedUnknownErrorException(AirflowFailException):
    """Raise if quanting failed with a unknown error state."""


class QuantingFailedException(AirflowFailException):
    """Raise if quanting failed but status has already been set."""
