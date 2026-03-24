"""Keys for accessing environmental variables."""


class ConstantsClass(type):
    """A metaclass for classes that should only contain string constants."""

    def __setattr__(cls, name: str, value: str) -> None:
        """Prevent modification of class attributes after they are set."""
        raise TypeError("Constants class cannot be modified")

    def get_values(cls) -> list[str]:
        """Get all user-defined string values of the class."""
        return [
            value
            for key, value in cls.__dict__.items()
            if not key.startswith("__") and isinstance(value, str)
        ]


class EnvVars(metaclass=ConstantsClass):
    """Keys for accessing docker environmental variables."""

    # the logic that depends on the environment should be as little as possible
    ENV_NAME = "ENV_NAME"

    MONGO_HOST = "MONGO_HOST"
    MONGO_PORT = "MONGO_PORT"
    MONGO_USER = "MONGO_USER"
    MONGO_PASSWORD = "MONGO_PASSWORD"  # noqa: S105  #Possible hardcoded password


class InstrumentTypes(metaclass=ConstantsClass):
    """Types of instruments."""

    THERMO: str = "thermo"
    BRUKER: str = "bruker"
    SCIEX: str = "sciex"


KNOWN_VENDOR_NAMES: tuple[str, ...] = tuple(InstrumentTypes.get_values())


class InternalPaths(metaclass=ConstantsClass):
    """Paths to directories within the Docker containers."""

    MOUNTS_PATH = "/opt/airflow/mounts/"
    ENVS_PATH = "/opt/airflow/envs/"

    INSTRUMENTS = "instruments"
    BACKUP = "backup"
    OUTPUT = "output"


class MetricsTypes(metaclass=ConstantsClass):
    """Types of metrics that can be added to a raw file."""

    ALPHADIA: str = "alphadia"
    MSQC: str = "msqc"
    SKYLINE: str = "skyline"
    CUSTOM: str = "custom"


class SoftwareTypes(metaclass=ConstantsClass):
    """Types of software that can be used for quanting."""

    ALPHADIA: str = "alphadia"
    CUSTOM: str = "custom"
    MSQC: str = "msqc"
    SKYLINE: str = "skyline"


DEFAULT_SCOPE = "*"

DDA_FLAG_IN_RAW_FILE_NAME = "_dda_"

# This a catch-all project ID assigned to raw files that can't be matched to any real project. It ensures every file has some project assignment so it can be stored
# and processed with default settings, rather than being rejected.
FALLBACK_PROJECT_ID = "_FALLBACK"
