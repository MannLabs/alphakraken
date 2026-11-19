"""Settings domain type and constants."""

import re
from dataclasses import dataclass
from datetime import datetime


class SettingsStatus:
    """Status of settings."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    DELETED = "deleted"


SETTINGS_NAME_REGEX = r"^[a-zA-Z0-9_]+$"
SLURM_MEM_REGEX = r"^\d+[KMGT]$"
SLURM_TIME_REGEX = r"^\d{2}:\d{2}:\d{2}$"

SETTINGS_VERSION_SEPARATOR = "_v"
_UNIQUE_SETTINGS_NAME_REGEX = re.compile(
    rf"^(?P<name>.+){SETTINGS_VERSION_SEPARATOR}(?P<version>[1-9][0-9]*)$"
)


def split_unique_settings_name(unique_name: str) -> tuple[str, int]:
    """Split a unique settings name like `fallback_v7` into name and version.

    Raises:
        ValueError: if `unique_name` does not have the expected format.

    """
    if (match := _UNIQUE_SETTINGS_NAME_REGEX.match(unique_name)) is None:
        raise ValueError(
            f"'{unique_name}' is not of the form <name>{SETTINGS_VERSION_SEPARATOR}<version>"
        )
    return match["name"], int(match["version"])


@dataclass(frozen=True)
class Settings:
    """Quanting settings. Datetimes are UTC-aware."""

    # the unique name, e.g. `fallback_v7`
    id: str
    # unique number for each settings
    number: int
    name: str
    version: int
    description: str | None
    fasta_file_name: str | None
    speclib_file_name: str | None
    config_file_name: str | None
    config_params: str | None
    software_type: str
    software: str
    # a name in alphakraken.yaml `runners`
    runner_name: str
    metrics_type: str
    slurm_cpus_per_task: int
    slurm_mem: str
    slurm_time: str
    num_threads: int
    status: str
    owner_email: str | None
    created_at_: datetime

    @property
    def unique_name(self) -> str:
        """Human-readable unique identifier, e.g. `fallback_v7`."""
        return self.id
