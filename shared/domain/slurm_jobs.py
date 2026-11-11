"""Slurm job domain type and constants."""

from dataclasses import dataclass
from datetime import datetime


class SlurmJobQueueStates:
    """States of Slurm jobs set by AlphaKraken, in addition to the ones reported by `squeue`."""

    # the job was in the queue, but is no more (e.g. finished)
    LEFT_QUEUE = "LEFT_QUEUE"


# expired entries are deleted by MongoDB
SLURM_JOB_EXPIRY_S = 7 * 24 * 60 * 60


@dataclass(frozen=True)
class SlurmJob:
    """State of a Slurm job as last reported by `squeue`. Datetimes are UTC-aware."""

    # "<cluster id>/<job id>", cf. `get_slurm_job_db_id`
    id: str
    # the SSH connection id prefix of the cluster
    cluster_id: str
    state: str
    updated_at_: datetime


def get_slurm_job_db_id(cluster_id: str, job_id: str) -> str:
    """Get the DB id of the Slurm job `job_id` on the cluster `cluster_id`, unique across clusters."""
    return f"{cluster_id}/{job_id}"
