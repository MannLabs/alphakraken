"""Settings domain type and constants."""

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Any, Self

from pydantic import (
    AfterValidator,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from shared.config_params import check_config_params
from shared.domain.inputs import (
    InputModel,
    OptionalStr,
    RequiredStr,
    empty_to_none,
    matching,
)
from shared.domain.users import OptionalLowerCasedEmail
from shared.keys import (
    DEFAULT_RESOURCE_PARAMS,
    SOFTWARE_TYPE_TO_DEFAULT_RESOURCE_PARAMS,
    SOFTWARE_TYPE_TO_METRICS_TYPES,
    MetricsTypes,
    SoftwareTypes,
)
from shared.validation import check_for_malicious_content


class SettingsStatus:
    """Status of settings."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    DELETED = "deleted"


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
    if (match := _UNIQUE_SETTINGS_NAME_REGEX.fullmatch(unique_name)) is None:
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


_RESOURCE_FIELDS = ("slurm_cpus_per_task", "slurm_mem", "slurm_time", "num_threads")


class NewSettings(InputModel):
    """Settings to create, cf. `Settings`.

    Empty optional strings are None; missing resource parameters are the defaults of the software type.
    """

    name: Annotated[
        RequiredStr,
        AfterValidator(
            matching(
                SETTINGS_NAME_REGEX, "may only contain letters, digits and underscores."
            )
        ),
    ] = Field(max_length=64, json_schema_extra={"pattern": SETTINGS_NAME_REGEX})
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
    slurm_mem: Annotated[
        OptionalStr,
        AfterValidator(
            matching(
                SLURM_MEM_REGEX,
                "Memory must be a number followed by a unit (K, M, G, or T), e.g. '62G'.",
            )
        ),
    ] = Field(
        default=None, max_length=16, json_schema_extra={"pattern": SLURM_MEM_REGEX}
    )
    slurm_time: Annotated[
        OptionalStr,
        AfterValidator(
            matching(SLURM_TIME_REGEX, "SLURM time must be in HH:MM:SS format.")
        ),
    ] = Field(
        default=None, max_length=16, json_schema_extra={"pattern": SLURM_TIME_REGEX}
    )
    num_threads: int | None = Field(default=None, ge=1)
    owner_email: OptionalLowerCasedEmail = None

    @model_validator(mode="before")
    @classmethod
    def _default_resources(cls, data: Any) -> Any:
        """Fill missing or empty resource parameters with the defaults of the software type."""
        if not isinstance(data, dict):
            return data
        software_type = data.get("software_type")
        # `get`, as an unknown or invalid software type is reported by its own rule
        defaults = SOFTWARE_TYPE_TO_DEFAULT_RESOURCE_PARAMS.get(
            software_type.strip() if isinstance(software_type, str) else "",
            DEFAULT_RESOURCE_PARAMS,
        )
        return {
            **data,
            **{
                field: getattr(defaults, field)
                for field in _RESOURCE_FIELDS
                if empty_to_none(data.get(field)) is None
            },
        }

    @field_validator("software_type")
    @classmethod
    def _known_software_type(cls, value: str) -> str:
        """Reject a software type that is not in `SoftwareTypes`."""
        if value not in SoftwareTypes.get_values():
            raise ValueError(
                f"Unknown software type '{value}', known are: {SoftwareTypes.get_values()}."
            )
        return value

    @field_validator("metrics_type")
    @classmethod
    def _metrics_type_fits_software_type(cls, value: str, info: ValidationInfo) -> str:
        """Reject a metrics type that the software type cannot have."""
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
        """Reject content that could be used for command injection."""
        if value and (errors := check_for_malicious_content(value)):
            raise ValueError("; ".join(errors))
        return value

    @field_validator("config_params")
    @classmethod
    def _safe_config_params(cls, value: str | None) -> str | None:
        """Reject unknown placeholders and content that could be used for command injection."""
        if value and (errors := check_config_params(value)):
            raise ValueError("; ".join(errors))
        return value

    @model_validator(mode="after")
    def _alphadia_needs_fasta_or_speclib(self) -> Self:
        """Reject alphadia settings with neither a fasta nor a speclib file name."""
        if (
            self.software_type == SoftwareTypes.ALPHADIA
            and not self.fasta_file_name
            and not self.speclib_file_name
        ):
            raise ValueError(
                "At least one of the fasta and speclib file names must be given."
            )
        return self
