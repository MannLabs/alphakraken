"""Shared helpers for all tests."""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from unittest.mock import patch

from shared.path_views import AIRFLOW_CONTAINER_VIEW, CLUSTER_VIEW


@contextmanager
def yaml_locations(**paths: str) -> Iterator[None]:
    """Override the locations of `CLUSTER_VIEW`, e.g. `yaml_locations(slurm="/path/to/slurm")`.

    Patches the contents of the view object rather than its name, so the patch reaches every
    import site and tests stay valid when code moves between modules.
    """
    with patch.object(
        CLUSTER_VIEW,
        "_locations",
        {key: PurePosixPath(path) for key, path in paths.items()},
    ):
        yield


@contextmanager
def container_locations(**paths: str) -> Iterator[None]:
    """Override the locations of `AIRFLOW_CONTAINER_VIEW`, e.g. `container_locations(output=str(tmp_path))`.

    Patches the contents of the view object rather than its name, so the patch reaches every
    import site and tests stay valid when code moves between modules.
    """
    with patch.object(
        AIRFLOW_CONTAINER_VIEW,
        "_locations",
        {key: Path(path) for key, path in paths.items()},
    ):
        yield
