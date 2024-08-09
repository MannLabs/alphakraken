"""Test thats DAGs are correctly loaded."""

from pathlib import Path

import pytest
from airflow.models import DagBag

from shared.settings import INSTRUMENTS

DAG_FOLDER = Path(__file__).parent / Path("../../dags")


@pytest.fixture()
def dagbag() -> DagBag:
    """Fixture for a DagBag instance with the DAGs loaded."""
    return DagBag(dag_folder=DAG_FOLDER, include_examples=False)


def test_dag_loaded_acquisition_watcher(dagbag: DagBag) -> None:
    """Test that acquisition_watcher loads correctly."""
    # when
    for instrument in INSTRUMENTS:
        dag = dagbag.get_dag(dag_id=f"acquisition_watcher.{instrument}")

        # then
        assert dagbag.import_errors == {}
        assert dag is not None
        assert len(dag.tasks) == 2  # noqa: PLR2004 no magic numbers


def test_dag_loaded_acquisition_handler(dagbag: DagBag) -> None:
    """Test that acquisition_watcher loads correctly."""
    # when
    for instrument in INSTRUMENTS:
        dag = dagbag.get_dag(dag_id=f"acquisition_handler.{instrument}")

        # then
        assert dagbag.import_errors == {}
        assert dag is not None
        assert len(dag.tasks) == 5  # noqa: PLR2004 no magic numbers
