"""Keys for accessing environmental variables."""

from dataclasses import dataclass


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


class SoftwareTypes(metaclass=ConstantsClass):
    """Types of software that can be used for quanting."""

    ALPHADIA: str = "alphadia"
    MSQC: str = "msqc"
    SKYLINE: str = "skyline"
    CUSTOM: str = "custom"


class MetricsTypes(metaclass=ConstantsClass):
    """Types of metrics that can be calculated from quanting results."""

    ALPHADIA: str = "alphadia"
    MSQC: str = "msqc"
    SKYLINE: str = "skyline"
    CUSTOM: str = "custom"


DEFAULT_SCOPE = "*"

DDA_FLAG_IN_RAW_FILE_NAME = "_dda_"

# This a catch-all project ID assigned to raw files that can't be matched to any real project. It ensures every file has some project assignment so it can be stored
# and processed with default settings, rather than being rejected.
FALLBACK_PROJECT_ID = "_FALLBACK"


@dataclass(frozen=True)
class SlurmParams:
    """Resource parameters for a Slurm job."""

    cpus_per_task: int
    mem: str
    time: str
    num_threads: int


SOFTWARE_TYPE_TO_DEFAULT_SLURM_PARAMS: dict[str, SlurmParams] = {
    SoftwareTypes.ALPHADIA: SlurmParams(
        cpus_per_task=8, mem="62G", time="02:00:00", num_threads=8
    ),
    SoftwareTypes.MSQC: SlurmParams(
        cpus_per_task=2, mem="31G", time="00:10:00", num_threads=2
    ),
    SoftwareTypes.SKYLINE: SlurmParams(
        cpus_per_task=2, mem="31G", time="00:10:00", num_threads=2
    ),
    SoftwareTypes.CUSTOM: SlurmParams(
        cpus_per_task=8, mem="62G", time="02:00:00", num_threads=8
    ),
}
