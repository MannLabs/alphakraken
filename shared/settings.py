"""Keys for accessing Dags, Tasks, etc.."""

INSTRUMENTS = {
    "test6": {},
    "test7": {},
}


class RawFileStatus:
    """Status of raw file."""

    NEW = "new"
    # have a distinction between processing and copying as network drives caused issues in the past.
    COPYING = "copying"
    PROCESSING = "processing"
    PROCESSED = "processed"
    FAILED = "failed"
