"""Tests for the settings validation helpers."""

from unittest.mock import MagicMock, patch

from service.settings_validation import check_runner_supports_software_type

from shared.keys import JobEngines, SoftwareTypes
from shared.runners import RUNNERS

_RUNNERS = {
    "cluster": MagicMock(engine=JobEngines.SLURM),
    "box": MagicMock(engine=JobEngines.DOCKER),
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


@patch.dict(RUNNERS, _RUNNERS, clear=True)
def test_slurm_runner_accepts_any_software_type() -> None:
    """Test that the rule is keyed by engine, not by runner name."""
    assert check_runner_supports_software_type("cluster", SoftwareTypes.ALPHADIA) == []
