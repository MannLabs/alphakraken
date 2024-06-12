"""Test thats DAGs are correctly loaded."""

from pathlib import Path
from unittest.mock import patch

import pytest
from airflow.models import DagBag
from plugins.common.settings import INSTRUMENTS

DAG_FOLDER = Path(__file__).parent / Path("../../dags")

# Note: mocking in this module is not straightforward, as in dagbag.py:347 modules are imported again


@pytest.fixture()
def dagbag() -> DagBag:
    """Fixture for a DagBag instance with the DAGs loaded."""
    with patch("airflow.providers.ssh.hooks.ssh.SSHHook"):
        return DagBag(dag_folder=DAG_FOLDER, include_examples=False)


def test_dag_load_acquisition_watcher(dagbag: DagBag) -> None:
    """Test that acquisition_watcher loads correctly."""
    # when
    for instrument in INSTRUMENTS:
        dag = dagbag.get_dag(dag_id=f"acquisition_watcher.{instrument}")

        # then
        assert dagbag.import_errors == {}
        assert dag is not None
        assert len(dag.tasks) == 3  # noqa: PLR2004 no magic numbers


def test_dag_load_acquisition_handler(dagbag: DagBag) -> None:
    """Test that acquisition_watcher loads correctly."""
    # when
    for instrument in INSTRUMENTS:
        dag = dagbag.get_dag(dag_id=f"acquisition_handler.{instrument}")

        # then
        assert dagbag.import_errors == {}
        assert dag is not None
        assert len(dag.tasks) == 7  # noqa: PLR2004 no magic numbers
