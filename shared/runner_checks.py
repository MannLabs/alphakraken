"""Checks of settings entries against the runners of alphakraken.yaml."""

from shared.keys import JobEngines, SoftwareTypes
from shared.runners import RUNNERS

# these engines run the `software` as given, which only the custom software type provides
_ENGINES_SUPPORTING_ONLY_CUSTOM = (
    JobEngines.DOCKER,
    JobEngines.SIMPLE_SSH,
    JobEngines.PUEUE_SSH,
)


def check_runner_supports_software_type(
    runner_name: str, software_type: str
) -> list[str]:
    """Check that the runner is declared in alphakraken.yaml and can run the software type."""
    if runner_name not in RUNNERS:
        return [
            f"Unknown runner '{runner_name}', declared in alphakraken.yaml are: {list(RUNNERS)}."
        ]
    engine = RUNNERS[runner_name].engine
    if (
        engine in _ENGINES_SUPPORTING_ONLY_CUSTOM
        and software_type != SoftwareTypes.CUSTOM
    ):
        return [
            f"Runner `{runner_name}` uses the `{engine}` engine, which only supports "
            f"software type `{SoftwareTypes.CUSTOM}`."
        ]
    return []
