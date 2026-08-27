"""Typed environment for a quanting job, passed from job preparation to job execution."""

from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field


class QuantingEnv(BaseModel):
    """Environment for a quanting job.

    The keys of the dictionary representation are the names of the environment variables that are
    set for the job script, a leading underscore marking the ones that are not exported to it.
    """

    model_config = ConfigDict(
        frozen=True, populate_by_name=True, extra="forbid", alias_generator=str.upper
    )

    raw_file_path: str
    settings_path: str
    output_path: str
    relative_output_path: str

    speclib_file_name: str | None
    fasta_file_name: str | None
    config_file_name: str | None
    software: str
    software_type: str
    metrics_type: str
    custom_command: str

    num_threads: int

    slurm_cpus_per_task: int = Field(alias="_SLURM_CPUS_PER_TASK")
    slurm_mem: str = Field(alias="_SLURM_MEM")
    slurm_time: str = Field(alias="_SLURM_TIME")

    raw_file_id: str
    project_id: str
    settings_name: str
    settings_version: int

    internal_output_path: str = Field(alias="_INTERNAL_OUTPUT_PATH")
    internal_raw_file_path: str = Field(alias="_INTERNAL_RAW_FILE_PATH")
    config_params: str = Field(alias="_CONFIG_PARAMS")
    job_engine: str = Field(alias="_JOB_ENGINE")

    def to_dict(self) -> dict[str, Any]:
        """Get the environment variable name to value mapping."""
        return self.model_dump(by_alias=True)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        """Create an instance from an environment variable name to value mapping."""
        return cls.model_validate(data)
