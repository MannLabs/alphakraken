"""Settings domain type and constants."""

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Self

from pydantic import (
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from shared.config_params import check_config_params
from shared.domain.inputs import InputModel, OptionalStr, RequiredStr
from shared.domain.users import OptionalLowerCasedEmail
from shared.keys import SOFTWARE_TYPE_TO_METRICS_TYPES, MetricsTypes, SoftwareTypes
from shared.validation import check_for_malicious_content


class SettingsStatus:
    """Status of settings."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    DELETED = "deleted"


# the statuses that may follow each status: archiving and deleting are final
SETTINGS_STATUS_TRANSITIONS: dict[str, set[str]] = {
    SettingsStatus.ACTIVE: {SettingsStatus.INACTIVE, SettingsStatus.DELETED},
}


SETTINGS_NAME_REGEX = r"^[a-zA-Z0-9_]+$"
# `[0-9]`, as `\d` also matches non-ASCII digits
SLURM_MEM_REGEX = r"^[0-9]+[KMGT]$"
SLURM_TIME_REGEX = r"^[0-9]{2}:[0-9]{2}:[0-9]{2}$"

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


def selectable_metrics_types(software_type: str) -> list[str]:
    """Return the metrics types settings of the software type can have, the default one first."""
    # metrics can always be switched off, cf. MetricsTypes.CUSTOM
    return list(
        dict.fromkeys(
            [*SOFTWARE_TYPE_TO_METRICS_TYPES[software_type], MetricsTypes.CUSTOM]
        )
    )


class NewSettings(InputModel):
    """Settings to create, cf. `Settings`. Empty optional strings are None."""

    name: RequiredStr = Field(max_length=64, pattern=SETTINGS_NAME_REGEX)
    software_type: RequiredStr
    software: RequiredStr = Field(max_length=128)
    runner_name: RequiredStr = Field(max_length=64)
    metrics_type: RequiredStr
    description: OptionalStr = Field(default=None, max_length=512)
    fasta_file_name: OptionalStr = Field(default=None, max_length=128)
    speclib_file_name: OptionalStr = Field(default=None, max_length=128)
    config_file_name: OptionalStr = Field(default=None, max_length=128)
    config_params: OptionalStr = Field(default=None, max_length=2048)
    slurm_cpus_per_task: int | None = Field(default=None, ge=1)
    slurm_mem: OptionalStr = Field(default=None, max_length=16)
    slurm_time: OptionalStr = Field(default=None, max_length=16)
    num_threads: int | None = Field(default=None, ge=1)
    owner_email: OptionalLowerCasedEmail = None

    @field_validator("software_type")
    @classmethod
    def _known_software_type(cls, value: str) -> str:
        if value not in SoftwareTypes.get_values():
            raise ValueError(
                f"Unknown software type '{value}', known are: {SoftwareTypes.get_values()}."
            )
        return value

    @field_validator("metrics_type")
    @classmethod
    def _metrics_type_fits_software_type(cls, value: str, info: ValidationInfo) -> str:
        # absent if the software type itself is invalid
        software_type = info.data.get("software_type")
        if software_type and value not in (
            allowed := selectable_metrics_types(software_type)
        ):
            raise ValueError(
                f"Metrics type '{value}' cannot be used with software type '{software_type}', "
                f"allowed are: {allowed}."
            )
        return value

    @field_validator(
        "software", "fasta_file_name", "speclib_file_name", "config_file_name"
    )
    @classmethod
    def _no_malicious_content(cls, value: str | None) -> str | None:
        if value and (errors := check_for_malicious_content(value)):
            raise ValueError("; ".join(errors))
        return value

    @field_validator("config_params")
    @classmethod
    def _safe_config_params(cls, value: str | None) -> str | None:
        # TODO: warn on bare (RAW_FILE_PATH) and half-open ({{RAW_FILE_PATH) placeholders,
        # they currently pass validation and fail silently at runtime
        if value and (errors := check_config_params(value)):
            raise ValueError("; ".join(errors))
        return value

    @field_validator("slurm_mem")
    @classmethod
    def _slurm_mem_format(cls, value: str | None) -> str | None:
        if value and not re.match(SLURM_MEM_REGEX, value):
            raise ValueError(
                "Memory must be a number followed by a unit (K, M, G, or T), e.g. '62G'."
            )
        return value

    @field_validator("slurm_time")
    @classmethod
    def _slurm_time_format(cls, value: str | None) -> str | None:
        if value and not re.match(SLURM_TIME_REGEX, value):
            raise ValueError("SLURM time must be in HH:MM:SS format.")
        return value

    @model_validator(mode="after")
    def _alphadia_needs_fasta_or_speclib(self) -> Self:
        if (
            self.software_type == SoftwareTypes.ALPHADIA
            and not self.fasta_file_name
            and not self.speclib_file_name
        ):
            raise ValueError(
                "At least one of the fasta and speclib file names must be given."
            )
        return self
