"""Typed environment for a quanting job."""

from pydantic import BaseModel, ConfigDict, Field


class QuantingEnv(BaseModel):
    """Environment of a quanting job, passed from `prepare_job` down to `store_metrics`.

    The aliases are the environment variable names that `cluster_scripts/submit_job.sh` reads,
    so renaming one requires adapting that script. Aliases with a leading underscore are not
    exported to the job.
    """

    model_config = ConfigDict(
        frozen=True, populate_by_name=True, extra="forbid", alias_generator=str.upper
    )

    raw_file_path: str
    settings_path: str
    output_path: str
    relative_output_path: str
    # not required in the settings, hence None-able
    speclib_file_name: str | None = None
    fasta_file_name: str | None = None
    config_file_name: str | None = None
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

    def to_dict(self) -> dict:
        """Get the environment as a dict keyed by environment variable name."""
        return self.model_dump(by_alias=True)

    @classmethod
    def from_dict(cls, data: dict) -> "QuantingEnv":
        """Create an environment from a dict keyed by environment variable name."""
        return cls.model_validate(data)
