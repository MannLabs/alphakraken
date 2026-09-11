"""Keys for accessing environmental variables."""

from collections import defaultdict
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

    KRAKEN_HOSTNAME = "KRAKEN_HOSTNAME"

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


class SoftwareTypes(metaclass=ConstantsClass):
    """Types of software that can be used for quanting."""

    ALPHADIA: str = "alphadia"
    MSQC: str = "msqc"
    SKYLINE: str = "skyline"
    CUSTOM: str = "custom"


class JobEngines(metaclass=ConstantsClass):
    """Execution engines that can run quanting jobs."""

    SLURM: str = "slurm"
    DOCKER: str = "docker"
    FILE_BASED: str = "file_based"


class MetricsTypes(metaclass=ConstantsClass):
    """Types of metrics that can be calculated from quanting results."""

    CUSTOM: str = "custom"
    ALPHADIA: str = "alphadia"
    MSQC: str = "msqc"
    SKYLINE: str = "skyline"
    DIANN: str = "diann"
    # EXAMPLE_METRICS: str = "example_metrics" # dummy code for adding new metrics


# The metrics types that can be selected for a software type, the first one being the default.
SOFTWARE_TYPE_TO_METRICS_TYPES: dict[str, list[str]] = {
    SoftwareTypes.ALPHADIA: [MetricsTypes.ALPHADIA],
    SoftwareTypes.MSQC: [MetricsTypes.MSQC],
    SoftwareTypes.SKYLINE: [MetricsTypes.SKYLINE],
    # SoftwareTypes.EXAMPLE: [MetricsTypes.EXAMPLE_METRICS],  # dummy code for adding new metrics
    # a custom software is not tied to any metrics, so all of them can be selected
    SoftwareTypes.CUSTOM: MetricsTypes.get_values(),
}


DEFAULT_SCOPE = "*"

DDA_FLAG_IN_RAW_FILE_NAME = "_dda_"

# This a catch-all project ID assigned to raw files that can't be matched to any real project. It ensures every file has some project assignment so it can be stored
# and processed with default settings, rather than being rejected.
FALLBACK_PROJECT_ID = "_FALLBACK"


@dataclass(frozen=True)
class ResourceParams:
    """Resource parameters, e.g. for a slurm jobs."""

    # slurm-specific
    slurm_cpus_per_task: int
    slurm_mem: str
    slurm_time: str
    # universal
    num_threads: int


DEFAULT_RESOURCE_PARAMS = ResourceParams(
    slurm_cpus_per_task=8, slurm_mem="62G", slurm_time="02:00:00", num_threads=8
)

SOFTWARE_TYPE_TO_DEFAULT_RESOURCE_PARAMS: dict[str, ResourceParams] = defaultdict(
    lambda: DEFAULT_RESOURCE_PARAMS,
    {
        SoftwareTypes.ALPHADIA: ResourceParams(
            slurm_cpus_per_task=8, slurm_mem="62G", slurm_time="02:00:00", num_threads=8
        ),
        SoftwareTypes.MSQC: ResourceParams(
            slurm_cpus_per_task=2, slurm_mem="31G", slurm_time="00:10:00", num_threads=2
        ),
        SoftwareTypes.SKYLINE: ResourceParams(
            slurm_cpus_per_task=2, slurm_mem="31G", slurm_time="00:10:00", num_threads=2
        ),
        SoftwareTypes.CUSTOM: ResourceParams(
            slurm_cpus_per_task=8, slurm_mem="62G", slurm_time="02:00:00", num_threads=8
        ),
    },
)
