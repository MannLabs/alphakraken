"""Tests for the domain.settings module."""

from typing import Any

import pytest

from shared.domain.errors import ValidationError
from shared.domain.inputs import parse
from shared.domain.settings import NewSettings

ALPHADIA: dict[str, Any] = {
    "name": "plasma_fast",
    "software_type": "alphadia",
    "software": "alphadia-2.0.0",
    "runner_name": "slurm",
    "metrics_type": "alphadia",
    "fasta_file_name": "human.fasta",
}
CUSTOM: dict[str, Any] = {
    "name": "diann",
    "software_type": "custom",
    "software": "diann/diann-linux",
    "runner_name": "slurm",
    "metrics_type": "diann",
    "config_params": "--f {{RAW_FILE_PATH}} --threads {{NUM_THREADS}}",
}


def test_new_settings_cleans_fields() -> None:
    """Strings are stripped, empty optional fields become None, resources are optional."""
    new = parse(
        NewSettings,
        **{**ALPHADIA, "name": " plasma_fast ", "description": " ", "slurm_mem": ""},
    )

    assert new.model_dump() == {
        **ALPHADIA,
        "description": None,
        "speclib_file_name": None,
        "config_file_name": None,
        "config_params": None,
        "slurm_cpus_per_task": None,
        "slurm_mem": None,
        "slurm_time": None,
        "num_threads": None,
        "owner_email": None,
    }


def test_new_settings_accepts_custom_software_with_resources() -> None:
    """A custom software may use any metrics type, placeholders and resource parameters."""
    new = parse(
        NewSettings,
        **CUSTOM,
        slurm_cpus_per_task=8,
        slurm_mem="62G",
        slurm_time="02:00:00",
        num_threads=8,
    )

    assert (new.slurm_mem, new.slurm_time) == ("62G", "02:00:00")


@pytest.mark.parametrize(
    ("overrides", "error"),
    [
        ({"name": "plasma fast"}, "name: String should match pattern"),
        ({"name": ""}, "name: String should match pattern"),
        ({"name": "n" * 65}, "name: String should have at most 64 characters"),
        ({"software_type": "other"}, "software_type: Unknown software type 'other'"),
        (
            {"metrics_type": "msqc"},
            "metrics_type: Metrics type 'msqc' cannot be used with software type 'alphadia'",
        ),
        ({"software": " "}, "software: String should have at least 1 character"),
        ({"software": "s" * 65}, "software: String should have at most 64 characters"),
        ({"software": "../bin/sh"}, r"software: Cannot contain '\.\.'"),
        ({"software": "/bin/sh"}, "software: Cannot be an absolute path"),
        ({"software": "a;b"}, "software: Contains invalid characters"),
        ({"fasta_file_name": "a b.fasta"}, "fasta_file_name: Contains invalid"),
        ({"speclib_file_name": "a$(b)"}, "speclib_file_name: Contains invalid"),
        ({"config_file_name": "c\\"}, "config_file_name: Cannot end with a backslash"),
        ({"runner_name": ""}, "runner_name: String should have at least 1 character"),
        ({"slurm_mem": "62"}, "slurm_mem: Memory must be a number followed by a unit"),
        ({"slurm_time": "2h"}, "slurm_time: SLURM time must be in HH:MM:SS format."),
        ({"slurm_cpus_per_task": 0}, "slurm_cpus_per_task: Input should be greater"),
        ({"num_threads": 0}, "num_threads: Input should be greater"),
        (
            {"fasta_file_name": ""},
            "^At least one of the fasta and speclib file names must be given.$",
        ),
    ],
)
def test_new_settings_rejects_invalid_fields(
    overrides: dict[str, Any], error: str
) -> None:
    """Each rule is reported once, with today's messages for the hand-written ones."""
    with pytest.raises(ValidationError, match=error) as e:
        parse(NewSettings, **{**ALPHADIA, **overrides})

    assert len(e.value.errors) == 1


@pytest.mark.parametrize(
    ("config_params", "error"),
    [
        ("--f {{RAW_FILE}}", "config_params: Unknown placeholder: {{RAW_FILE}}"),
        ("--f {RAW_FILE_PATH}", "config_params: Unknown placeholder: {RAW_FILE_PATH}"),
        ("--f a; rm -rf x", "config_params: Contains invalid characters"),
        ("--f {{RAW_FILE_PATH}} && ls", "config_params: Contains invalid characters"),
    ],
)
def test_new_settings_rejects_unsafe_config_params(
    config_params: str, error: str
) -> None:
    """Unknown placeholders are reported first, then content that is unsafe after substitution."""
    with pytest.raises(ValidationError) as e:
        parse(NewSettings, **{**CUSTOM, "config_params": config_params})

    assert len(e.value.errors) == 1
    assert e.value.errors[0].startswith(error)


def test_new_settings_reports_all_invalid_fields() -> None:
    """Several invalid fields are reported together."""
    with pytest.raises(ValidationError) as e:
        parse(NewSettings, **{**ALPHADIA, "software": "/x", "slurm_time": "soon"})

    assert e.value.errors == [
        "software: Cannot be an absolute path",
        "slurm_time: SLURM time must be in HH:MM:SS format.",
    ]


def test_new_settings_alphadia_accepts_speclib_only() -> None:
    """A speclib alone satisfies the alphadia input rule."""
    fields = {**ALPHADIA, "fasta_file_name": None, "speclib_file_name": "lib.speclib"}

    assert parse(NewSettings, **fields).speclib_file_name == "lib.speclib"
