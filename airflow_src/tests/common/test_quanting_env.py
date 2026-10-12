"""Tests for the quanting_env module."""

from collections.abc import Callable

from common.quanting_env import QuantingEnv

# The aliases are a contract with consumers outside this code base and must not change
# silently: `cluster_scripts/submit_job.sh` reads the exported ones off the environment,
# the docker engine passes them into the container, and the file-based engine writes some
# of them into the `.job` file read by an external watcher.
# Renaming a field changes its alias, because `alias_generator=str.upper` derives it.
EXPECTED_ALIASES = {
    "raw_file_path": "RAW_FILE_PATH",
    "settings_path": "SETTINGS_PATH",
    "output_path": "OUTPUT_PATH",
    "relative_output_path": "RELATIVE_OUTPUT_PATH",
    "speclib_file_name": "SPECLIB_FILE_NAME",
    "fasta_file_name": "FASTA_FILE_NAME",
    "config_file_name": "CONFIG_FILE_NAME",
    "software": "SOFTWARE",
    "software_type": "SOFTWARE_TYPE",
    "metrics_type": "METRICS_TYPE",
    "custom_command": "CUSTOM_COMMAND",
    "slurm_cpus_per_task": "_SLURM_CPUS_PER_TASK",
    "slurm_mem": "_SLURM_MEM",
    "slurm_time": "_SLURM_TIME",
    "num_threads": "NUM_THREADS",
    "raw_file_id": "RAW_FILE_ID",
    "project_id": "PROJECT_ID",
    "settings_name": "SETTINGS_NAME",
    "settings_version": "SETTINGS_VERSION",
    "config_params": "_CONFIG_PARAMS",
    "relative_raw_file_path": "_RELATIVE_RAW_FILE_PATH",
    "runner_name": "_RUNNER_NAME",
}

# the subset that reaches the quanting job, cf. the leading-underscore filter in the job handlers
EXPECTED_EXPORTED_ALIASES = {
    "RAW_FILE_PATH",
    "SETTINGS_PATH",
    "OUTPUT_PATH",
    "RELATIVE_OUTPUT_PATH",
    "SPECLIB_FILE_NAME",
    "FASTA_FILE_NAME",
    "CONFIG_FILE_NAME",
    "SOFTWARE",
    "SOFTWARE_TYPE",
    "METRICS_TYPE",
    "CUSTOM_COMMAND",
    "NUM_THREADS",
    "RAW_FILE_ID",
    "PROJECT_ID",
    "SETTINGS_NAME",
    "SETTINGS_VERSION",
}


def test_aliases_are_the_expected_environment_variable_names() -> None:
    """Test that every field carries the environment variable name its consumers expect."""
    aliases = {name: field.alias for name, field in QuantingEnv.model_fields.items()}

    assert aliases == EXPECTED_ALIASES


def test_exported_aliases_are_the_ones_without_leading_underscore() -> None:
    """Test that exactly the expected aliases reach the quanting job."""
    exported = {
        field.alias
        for field in QuantingEnv.model_fields.values()
        if not str(field.alias).startswith("_")
    }

    assert exported == EXPECTED_EXPORTED_ALIASES


def test_to_dict_is_keyed_by_alias(
    make_quanting_env: Callable[..., QuantingEnv],
) -> None:
    """Test that the XCom payload uses the environment variable names as keys."""
    assert set(make_quanting_env().to_dict()) == set(EXPECTED_ALIASES.values())
