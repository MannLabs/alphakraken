"""Custom exceptions that are handled by callbacks."""

from airflow.exceptions import AirflowFailException


class RawfileProcessingError(AirflowFailException):
    """Base class for all raw file processing errors.

    Inherits from AirflowFailException, which means that no retries are performed on the raising task.
    """

    def __init__(self, raw_file_name: str, msg: str, details: str | None = None):
        """Initializes the error.

        :param raw_file_name: name of raw file. Must match the name in the database.
        :param msg: Error message
        :param details: details of the error
        """
        self.raw_file_name = raw_file_name
        self.msg = msg
        self.details = details


class QuantingFailedError(RawfileProcessingError):
    """Raised when quanting fails."""

    def __init__(self, raw_file_name: str, details: str | None = None):
        """Initializes the error."""
        super().__init__(raw_file_name, msg="Quanting failed", details=details)
