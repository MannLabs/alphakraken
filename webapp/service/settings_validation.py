"""Validation of settings entries against the runners of alphakraken.yaml."""

from shared.keys import JobEngines, SoftwareTypes
from shared.runners import RUNNERS


def check_runner_supports_software_type(
    runner_name: str, software_type: str
) -> list[str]:
    """Check that the software type can run on the given runner.

    The docker engine runs an image given as `software`, which only the custom software type provides.
    """
    if (
        RUNNERS[runner_name].engine == JobEngines.DOCKER
        and software_type != SoftwareTypes.CUSTOM
    ):
        return [
            f"Runner `{runner_name}` uses the `{JobEngines.DOCKER}` engine, which only supports "
            f"software type `{SoftwareTypes.CUSTOM}`."
        ]
    return []
