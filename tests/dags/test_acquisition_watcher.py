"""Test the acquisition_watcher DAG."""

from pathlib import Path

import pytest
from airflow.models import DagBag

DAG_FOLDER = Path(__file__).parent / Path("../../dags")


@pytest.fixture()
def dagbag() -> DagBag:
    """Fixture for a DagBag instance with the DAGs loaded."""
    return DagBag(dag_folder=DAG_FOLDER, include_examples=False)


def test_dag_loaded_acquisition_watcher(dagbag: DagBag) -> None:
    """Test that acquisition_watcher loads correctly."""
    # when
    dag = dagbag.get_dag(dag_id="acquisition_watcher.test6")

    # then
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 2  # noqa: PLR2004 no magic numbers
