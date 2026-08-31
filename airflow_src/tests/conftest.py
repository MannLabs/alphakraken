"""Shared code for all tests."""

import os

os.environ["ENV_NAME"] = "_test_"

from collections.abc import Callable

import pytest
from common.quanting_env import QuantingEnv

_QUANTING_ENV_DEFAULTS = {
    "raw_file_path": "/pool/backup/instrument1/1970_01/test_file.raw",
    "settings_path": "/pool/settings/test_settings",
    "output_path": "/pool/output/PID1/out_test_file.raw/alphadia",
    "relative_output_path": "PID1/out_test_file.raw/alphadia",
    "speclib_file_name": "some_speclib_file_name",
    "fasta_file_name": "some_fasta_file_name",
    "config_file_name": "some_config_file_name",
    "software": "some_software",
    "software_type": "alphadia",
    "metrics_type": "alphadia",
    "custom_command": "",
    "slurm_cpus_per_task": 8,
    "slurm_mem": "62G",
    "slurm_time": "02:00:00",
    "num_threads": 8,
    "raw_file_id": "test_file.raw",
    "project_id": "PID1",
    "settings_name": "test_settings",
    "settings_version": 1,
    "internal_output_path": "/opt/airflow/mounts/output/PID1/out_test_file.raw/alphadia",
    "internal_raw_file_path": "/opt/airflow/mounts/backup/instrument1/1970_01/test_file.raw",
    "config_params": "",
    "job_engine": "slurm",
    "year_month_folder": "1970_01",
}


@pytest.fixture
def make_quanting_env() -> Callable[..., QuantingEnv]:
    """Return a factory for a fully populated QuantingEnv, overridable per field."""

    def factory(**overrides: object) -> QuantingEnv:
        return QuantingEnv(**{**_QUANTING_ENV_DEFAULTS, **overrides})  # type: ignore[missing-argument]

    return factory
