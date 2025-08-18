"""Keys for accessing environmental variables."""


class EnvVars:
    """Keys for accessing docker environmental variables."""

    # the logic that depends on the environment should be as little as possible
    ENV_NAME = "ENV_NAME"

    MONGO_HOST = "MONGO_HOST"
    MONGO_PORT = "MONGO_PORT"
    MONGO_USER = "MONGO_USER"
    MONGO_PASSWORD = "MONGO_PASSWORD"  # noqa: S105  #Possible hardcoded password

    MESSENGER_WEBHOOK_URL = "MESSENGER_WEBHOOK_URL"  # TODO: move to yaml


class InstrumentTypes:
    """Types of instruments."""

    THERMO: str = "thermo"
    BRUKER: str = "bruker"
    SCIEX: str = "sciex"


class InternalPaths:
    """Paths to directories within the Docker containers."""

    MOUNTS_PATH = "/opt/airflow/mounts/"
    ENVS_PATH = "/opt/airflow/envs/"

    INSTRUMENTS = "instruments"
    BACKUP = "backup"
    OUTPUT = "output"


class MetricsTypes:
    """Types of metrics that can be added to a raw file."""

    ALPHADIA: str = "alphadia"
    CUSTOM: str = "custom"


class SoftwareTypes:
    """Types of software that can be used for quanting."""

    ALPHADIA: str = "alphadia"
    CUSTOM: str = "custom"


_ALLOWED_CHARACTERS = r"a-zA-Z0-9\-_+\."
ALLOWED_CHARACTERS_PRETTY = _ALLOWED_CHARACTERS.replace("\\", "")
FORBIDDEN_CHARACTERS_REGEXP = rf"[^{_ALLOWED_CHARACTERS}]"

DDA_FLAG_IN_RAW_FILE_NAME = "_dda_"
