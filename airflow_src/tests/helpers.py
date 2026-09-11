"""Shared helpers for all tests."""

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from unittest.mock import patch

from shared.path_views import AIRFLOW_CONTAINER_VIEW
from shared.runners import RUNNERS


@contextmanager
def runner_view(runner_name: str, **paths: str) -> Iterator[None]:
    """Override the view of a declared runner, e.g. `runner_view("slurm", backup="/pool/backup")`.

    Patches the contents of the view object rather than its name, so the patch reaches every
    import site and tests stay valid when code moves between modules.
    """
    with patch.object(
        RUNNERS[runner_name].view,
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
