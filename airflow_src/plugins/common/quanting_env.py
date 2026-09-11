"""The environment of a quanting job."""

from pydantic import BaseModel, ConfigDict, Field


class QuantingEnv(BaseModel):
    """Environment of a quanting job.

    The field aliases are the environment variable names as read by `cluster_scripts/submit_job.sh`,
    e.g. `raw_file_path -> RAW_FILE_PATH`.
    Aliases with a leading underscore are not exported to the job, cf. the job handlers.
    """

    model_config = ConfigDict(
        frozen=True, populate_by_name=True, extra="forbid", alias_generator=str.upper
    )

    # do not rename any field names without updating the aliases, see module docstring.
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

    slurm_cpus_per_task: int = Field(alias="_SLURM_CPUS_PER_TASK")
    slurm_mem: str = Field(alias="_SLURM_MEM")
    slurm_time: str = Field(alias="_SLURM_TIME")
    num_threads: int

    raw_file_id: str
    project_id: str
    settings_name: str
    settings_version: int

    internal_output_path: str = Field(alias="_INTERNAL_OUTPUT_PATH")
    internal_raw_file_path: str = Field(alias="_INTERNAL_RAW_FILE_PATH")
    config_params: str = Field(alias="_CONFIG_PARAMS")
    job_engine: str = Field(alias="_JOB_ENGINE")
    year_month_folder: str = Field(alias="_YEAR_MONTH_FOLDER")

    def to_dict(self) -> dict:
        """Convert to a dict keyed by the environment variable names."""
        return self.model_dump(by_alias=True)

    @classmethod
    def from_dict(cls, data: dict) -> "QuantingEnv":
        """Create from a dict keyed by the environment variable names."""
        return cls.model_validate(data)
