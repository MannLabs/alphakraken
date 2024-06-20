"""Keys for accessing environmental variables."""


class EnvVars:
    """Keys for accessing docker environmental variables."""

    INSTRUMENT_PATH_TEST1 = "INSTRUMENT_PATH_TEST1"
    INSTRUMENT_PATH_ASTRAL1 = "INSTRUMENT_PATH_ASTRAL1"
    INSTRUMENT_PATH_ASTRAL2 = "INSTRUMENT_PATH_ASTRAL2"

    MONGO_HOST = "MONGO_HOST"
    MONGO_PORT = "MONGO_PORT"
    MONGO_USER = "MONGO_USER"
    MONGO_PASSWORD = "MONGO_PASSWORD"  # noqa: S105  #Possible hardcoded password

    IO_POOL_FOLDER = "IO_POOL_FOLDER"
