"""Keys for accessing environmental variables."""


class EnvVars:
    """Keys for accessing docker environmental variables for the shared part."""

    MONGO_HOST = "MONGO_HOST"
    MONGO_PORT = "MONGO_PORT"
    MONGO_USER = "MONGO_USER"
    MONGO_PASSWORD = "MONGO_PASSWORD"  # noqa: S105  #Possible hardcoded password
