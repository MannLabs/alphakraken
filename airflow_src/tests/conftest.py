"""Shared code for all tests."""

import os
from collections.abc import Callable
from typing import Any

import pytest

os.environ["ENV_NAME"] = "_test_"

from common.quanting_env import QuantingEnv

_QUANTING_ENV_DEFAULTS: dict[str, Any] = {
    "raw_file_path": "/pool/backup/instrument1/2024_07/test_file.raw",
    "settings_path": "/pool/settings/test_settings",
    "output_path": "/pool/output/PID123/out_test_file.raw/alphadia",
    "relative_output_path": "PID123/out_test_file.raw/alphadia",
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
    "project_id": "PID123",
    "settings_name": "test_settings",
    "settings_version": 1,
    "internal_output_path": "/opt/airflow/mounts/output/PID123/out_test_file.raw/alphadia",
    "internal_raw_file_path": "/opt/airflow/mounts/backup/instrument1/2024_07/test_file.raw",
    "config_params": "",
    "job_engine": "slurm",
}


@pytest.fixture
def make_quanting_env() -> Callable[..., QuantingEnv]:
    """Get a factory for a fully populated QuantingEnv, overridable per field name."""

    def _make(**overrides: Any) -> QuantingEnv:
        return QuantingEnv(**(_QUANTING_ENV_DEFAULTS | overrides))

    return _make
