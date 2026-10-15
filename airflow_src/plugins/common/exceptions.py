"""Custom Airflow exceptions."""

from airflow.sdk.exceptions import AirflowFailException, AirflowSkipException

# TODO: move all custom exceptions here


class QuantingFailedNewErrorException(AirflowFailException):
    """Raise if quanting failed with a new error."""


class QuantingFailedKnownErrorException(AirflowSkipException):
    """Raise if quanting failed with a known error."""


class QuantingFailedUnknownErrorException(AirflowFailException):
    """Raise if quanting failed with a unknown error state."""


class QuantingFailedException(AirflowFailException):
    """Raise if quanting failed but status has already been set."""


class S3UploadFailedException(AirflowFailException):
    """Exception raised when S3 upload fails.

    Enables on_failure_callback to take special action.
    """
