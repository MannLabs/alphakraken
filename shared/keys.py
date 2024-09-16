"""Keys for accessing environmental variables."""


class EnvVars:
    """Keys for accessing docker environmental variables."""

    # the logic that depends on the environment should be as little as possible
    ENV_NAME = "ENV_NAME"

    MONGO_HOST = "MONGO_HOST"
    MONGO_PORT = "MONGO_PORT"
    MONGO_USER = "MONGO_USER"
    MONGO_PASSWORD = "MONGO_PASSWORD"  # noqa: S105  #Possible hardcoded password

    POOL_BASE_PATH = "POOL_BASE_PATH"
    BACKUP_POOL_FOLDER = "BACKUP_POOL_FOLDER"
    QUANTING_POOL_FOLDER = "QUANTING_POOL_FOLDER"


ALLOWED_CHARACTERS_IN_RAW_FILE_NAME = r"[^a-zA-Z0-9\-_+.]"
DDA_FLAG_IN_RAW_FILE_NAME = "_dda_"
