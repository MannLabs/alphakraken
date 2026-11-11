"""Tests for the runner_checks module."""

from unittest.mock import MagicMock, patch

import pytest

from shared.keys import JobEngines, SoftwareTypes
from shared.runner_checks import check_runner_supports_software_type
from shared.runners import RUNNERS

_RUNNERS = {
    "cluster": MagicMock(engine=JobEngines.SLURM),
    "box": MagicMock(engine=JobEngines.DOCKER),
    "ssh_box": MagicMock(engine=JobEngines.SIMPLE_SSH),
    "pueue_box": MagicMock(engine=JobEngines.PUEUE_SSH),
}


@patch.dict(RUNNERS, _RUNNERS, clear=True)
def test_docker_runner_rejects_non_custom_software_type() -> None:
    """Test that a runner with the docker engine only accepts the custom software type."""
    errors = check_runner_supports_software_type("box", SoftwareTypes.ALPHADIA)

    assert len(errors) == 1
    assert "box" in errors[0]


@patch.dict(RUNNERS, _RUNNERS, clear=True)
def test_docker_runner_accepts_custom_software_type() -> None:
    """Test that the docker engine with the custom software type passes."""
    assert check_runner_supports_software_type("box", SoftwareTypes.CUSTOM) == []


@pytest.mark.parametrize(
    ("runner_name", "engine"),
    [("ssh_box", JobEngines.SIMPLE_SSH), ("pueue_box", JobEngines.PUEUE_SSH)],
)
@patch.dict(RUNNERS, _RUNNERS, clear=True)
def test_plain_machine_runner_rejects_non_custom_software_type(
    runner_name: str, engine: str
) -> None:
    """Test that the engines for plain machines only accept the custom software type, naming the engine."""
    errors = check_runner_supports_software_type(runner_name, SoftwareTypes.ALPHADIA)

    assert len(errors) == 1
    assert runner_name in errors[0]
    assert engine in errors[0]


@pytest.mark.parametrize("runner_name", ["ssh_box", "pueue_box"])
@patch.dict(RUNNERS, _RUNNERS, clear=True)
def test_plain_machine_runner_accepts_custom_software_type(runner_name: str) -> None:
    """Test that the engines for plain machines with the custom software type pass."""
    assert check_runner_supports_software_type(runner_name, SoftwareTypes.CUSTOM) == []


@patch.dict(RUNNERS, _RUNNERS, clear=True)
def test_slurm_runner_accepts_any_software_type() -> None:
    """Test that the rule is keyed by engine, not by runner name."""
    assert check_runner_supports_software_type("cluster", SoftwareTypes.ALPHADIA) == []


@patch.dict(RUNNERS, _RUNNERS, clear=True)
def test_unknown_runner_is_rejected() -> None:
    """Test that a runner not declared in the yaml is an error, not an exception."""
    errors = check_runner_supports_software_type("nowhere", SoftwareTypes.CUSTOM)

    assert len(errors) == 1
    assert "Unknown runner 'nowhere'" in errors[0]
