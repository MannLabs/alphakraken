"""Slurm job queries."""

from shared.db._mongo import slurm_jobs as _backend
from shared.domain.slurm_jobs import SlurmJob


def upsert_states(cluster_id: str, states: dict[str, str]) -> None:
    """Store the `states` (job id -> state) of all jobs in the queue of the cluster `cluster_id`.

    Jobs of the cluster that are stored but not in `states` get the state `LEFT_QUEUE`.
    """
    _backend.upsert_states(cluster_id, states)


def get(cluster_id: str, job_id: str) -> SlurmJob | None:
    """Return the Slurm job `job_id` of the cluster `cluster_id`, None if it was never seen in the queue."""
    return _backend.get(cluster_id, job_id)
