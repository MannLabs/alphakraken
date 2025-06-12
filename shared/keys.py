"""Keys for accessing environmental variables."""


class EnvVars:
    """Keys for accessing docker environmental variables."""

    # the logic that depends on the environment should be as little as possible
    ENV_NAME = "ENV_NAME"

    MONGO_HOST = "MONGO_HOST"
    MONGO_PORT = "MONGO_PORT"
    MONGO_USER = "MONGO_USER"
    MONGO_PASSWORD = "MONGO_PASSWORD"  # noqa: S105  #Possible hardcoded password
    MONGO_USER_READ = "MONGO_USER_READ"
    MONGO_PASSWORD_READ = "MONGO_PASSWORD_READ"  # noqa: S105  #Possible hardcoded password
    MONGO_USER_WEBAPP = "MONGO_USER_WEBAPP"
    MONGO_PASSWORD_WEBAPP = "MONGO_PASSWORD_WEBAPP"  # noqa: S105  #Possible hardcoded password

    MESSENGER_WEBHOOK_URL = "MESSENGER_WEBHOOK_URL"  # TODO: move to yaml


class InstrumentTypes:
    """Types of instruments."""

    THERMO: str = "thermo"
    BRUKER: str = "bruker"
    SCIEX: str = "sciex"


class Locations:
    """Keys for accessing paths in the yaml config."""

    BACKUP = "backup"
    SETTINGS = "settings"
    OUTPUT = "output"
    SLURM = "slurm"


class MetricsTypes:
    """Types of metrics that can be added to a raw file."""

    ALPHADIA: str = "alphadia"
    CUSTOM: str = "custom"


ALLOWED_CHARACTERS_IN_RAW_FILE_NAME = r"[^a-zA-Z0-9\-_+.]"
DDA_FLAG_IN_RAW_FILE_NAME = "_dda_"
