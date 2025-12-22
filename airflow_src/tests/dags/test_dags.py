"""Test thats DAGs are correctly loaded."""

from pathlib import Path
from unittest.mock import patch

import pytest
from airflow.models import Connection, DagBag

DAG_FOLDER = Path(__file__).parent / Path("../../dags")

# Note: mocking in this module is not straightforward, as in dagbag.py:347 modules are imported again


@pytest.fixture
def fixture_cluster_ssh_connection_uri() -> str:
    """Fixture for a mock cluster SSH connection URI."""
    mock_cluster_ssh_connection = Connection(
        conn_type="ssh",
        host="mock-conn-host",
    )
    return mock_cluster_ssh_connection.get_uri()


@pytest.fixture
def dagbag(fixture_cluster_ssh_connection_uri: str) -> DagBag:
    """Fixture for a DagBag instance with the DAGs loaded."""
    with (
        patch.dict(
            "os.environ",
            AIRFLOW_CONN_CLUSTER_SSH_CONNECTION=fixture_cluster_ssh_connection_uri,
            ENV_NAME="_test_",
        ),
    ):
        return DagBag(dag_folder=DAG_FOLDER, include_examples=False)


def test_dag_load_instrument_watcher(dagbag: DagBag) -> None:
    """Test that instrument_watcher loads correctly."""
    # when
    for instrument in [
        "_test1_",
    ]:
        dag = dagbag.get_dag(dag_id=f"instrument_watcher.{instrument}")

        # then
        assert dagbag.import_errors == {}
        assert dag is not None
        assert len(dag.tasks) == 4


def test_dag_load_acquisition_handler(dagbag: DagBag) -> None:
    """Test that acquisition_handler loads correctly."""
    # when
    for instrument in [
        "_test1_",
    ]:
        dag = dagbag.get_dag(dag_id=f"acquisition_handler.{instrument}")

        # then
        assert dagbag.import_errors == {}
        assert dag is not None
        assert len(dag.tasks) == 6


def test_dag_load_acquisition_processor(dagbag: DagBag) -> None:
    """Test that instrument_watcher loads correctly."""
    # when
    for instrument in [
        "_test1_",
    ]:
        dag = dagbag.get_dag(dag_id=f"acquisition_processor.{instrument}")

        # then
        assert dagbag.import_errors == {}
        assert dag is not None
        assert len(dag.tasks) == 7


def test_dag_load_file_mover(dagbag: DagBag) -> None:
    """Test that file_mover loads correctly."""
    # when
    for instrument in [
        "_test1_",
    ]:
        dag = dagbag.get_dag(dag_id=f"file_mover.{instrument}")

        # then
        assert dagbag.import_errors == {}
        assert dag is not None
        assert len(dag.tasks) == 2


def test_dag_load_file_remover(dagbag: DagBag) -> None:
    """Test that file_remover loads correctly."""
    # when
    dag = dagbag.get_dag(dag_id="file_remover")

    # then
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 2
