"""Keys for accessing Dags, Tasks, etc.."""

INSTRUMENTS = {
    "test6": {},
    "test7": {},
}


class RawFileStatus:
    """Status of raw file."""

    NEW = "new"
    PROCESSING = "processing"
    PROCESSED = "processed"
    FAILED = "failed"
