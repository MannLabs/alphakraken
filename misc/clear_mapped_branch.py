"""Clear all tasks in the processing task group for a specific map_index.

Workaround for Airflow bug https://github.com/apache/airflow/issues/41278
where clearing a mapped task with "downstream" clears ALL map_indexes
instead of just the targeted one.

Run inside the Airflow container:
    python misc/clear_mapped_branch.py <dag_id> <dag_run_id> <map_index> [--dry-run] [--include-finalize]

Example:
    python misc/clear_mapped_branch.py acquisition_processor.instrument1 manual__2024-01-15T10:00:00+00:00 1 --dry-run
    python misc/clear_mapped_branch.py acquisition_processor.instrument1 manual__2024-01-15T10:00:00+00:00 1

"""

# ruff: noqa: T201
from __future__ import annotations

import argparse
import sys

from airflow.models import DagRun
from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState

PROCESSING_PREFIX = "processing."
FINALIZE_TASK_ID = "finalize_status"


def _check_db_connection() -> None:
    """Verify that the Airflow metadata DB is reachable."""
    try:
        with create_session() as session:
            session.execute("SELECT 1")
    except Exception as e:  # noqa: BLE001
        print(
            f"ERROR: Cannot connect to Airflow metadata DB: {e}\n\n"
            "This script must run inside an Airflow container where\n"
            "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN is configured."
        )
        sys.exit(1)


def clear_mapped_branch(
    dag_id: str,
    dag_run_id: str,
    map_index: int,
    *,
    dry_run: bool = False,
    include_finalize: bool = False,
) -> None:
    """Clear all task instances in the processing group for a specific map_index."""
    _check_db_connection()
    with create_session() as session:
        dag_run = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == dag_id,
                DagRun.run_id == dag_run_id,
            )
            .first()
        )

        if not dag_run:
            recent_runs = (
                session.query(DagRun.run_id)
                .filter(DagRun.dag_id == dag_id)
                .order_by(DagRun.execution_date.desc())
                .limit(5)
                .all()
            )
            print(f"ERROR: DAG run not found: {dag_id} / {dag_run_id}")
            if recent_runs:
                print(f"\nRecent runs for '{dag_id}':")
                for (run_id,) in recent_runs:
                    print(f"  {run_id}")
            else:
                print(f"\nNo DAG runs found for '{dag_id}' at all. Check the dag_id.")
            sys.exit(1)

        tis = (
            session.query(TaskInstance)
            .filter(
                TaskInstance.dag_id == dag_id,
                TaskInstance.run_id == dag_run_id,
                TaskInstance.map_index == map_index,
                TaskInstance.task_id.like(f"{PROCESSING_PREFIX}%"),
            )
            .all()
        )

        if include_finalize:
            finalize_ti = (
                session.query(TaskInstance)
                .filter(
                    TaskInstance.dag_id == dag_id,
                    TaskInstance.run_id == dag_run_id,
                    TaskInstance.task_id == FINALIZE_TASK_ID,
                )
                .first()
            )
            if finalize_ti:
                tis.append(finalize_ti)

        if not tis:
            print(f"No task instances found for map_index={map_index}")
            return

        print(f"Task instances to clear ({len(tis)}):")
        for ti in tis:
            print(f"  {ti.task_id}[{ti.map_index}] state={ti.state}")

        if dry_run:
            print("\n[DRY RUN] No changes made.")
            return

        clear_task_instances(
            tis=tis,
            session=session,
            dag_run_state=DagRunState.QUEUED,
        )

        print(f"\nCleared {len(tis)} task instances for map_index={map_index}.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Clear all tasks in a mapped branch by map_index. "
        "Workaround for https://github.com/apache/airflow/issues/41278",
    )
    parser.add_argument(
        "dag_id", help="DAG ID (e.g. acquisition_processor.instrument_1)"
    )
    parser.add_argument("dag_run_id", help="DAG run ID")
    parser.add_argument("map_index", type=int, help="map_index to clear")
    parser.add_argument(
        "--dry-run", action="store_true", help="Preview which tasks would be cleared"
    )
    parser.add_argument(
        "--include-finalize",
        action="store_true",
        help="Also clear the finalize_status task (downstream of the task group)",
    )

    args = parser.parse_args()
    clear_mapped_branch(
        dag_id=args.dag_id,
        dag_run_id=args.dag_run_id,
        map_index=args.map_index,
        dry_run=args.dry_run,
        include_finalize=args.include_finalize,
    )
