"""Slurm job queries on MongoDB."""

from datetime import UTC, datetime

from pymongo import UpdateOne

from shared.db._mongo import models
from shared.db._mongo.connection import ensure_connected
from shared.db._mongo.errors import translate_errors
from shared.db._mongo.mappers import to_slurm_job
from shared.domain.slurm_jobs import (
    SlurmJob,
    SlurmJobQueueStates,
    get_slurm_job_db_id,
)


@translate_errors
def upsert_states(cluster_id: str, states: dict[str, str]) -> None:
    """Store the states of the queued jobs of a cluster, cf. `shared.db.slurm_jobs`."""
    ensure_connected()
    now = datetime.now(tz=UTC)
    db_ids = [get_slurm_job_db_id(cluster_id, job_id) for job_id in states]

    if states:
        # one round trip for all jobs
        models.SlurmJob._get_collection().bulk_write(  # noqa: SLF001
            [
                UpdateOne(
                    {"_id": db_id},
                    {
                        "$set": {
                            "cluster_id": cluster_id,
                            "state": state,
                            "updated_at_": now,
                        }
                    },
                    upsert=True,
                )
                for db_id, state in zip(db_ids, states.values(), strict=True)
            ],
            ordered=False,
        )

    models.SlurmJob.objects(
        cluster_id=cluster_id,
        state__ne=SlurmJobQueueStates.LEFT_QUEUE,
        id__nin=db_ids,
    ).update(set__state=SlurmJobQueueStates.LEFT_QUEUE, set__updated_at_=now)


@translate_errors
def get(cluster_id: str, job_id: str) -> SlurmJob | None:
    """Return a Slurm job, cf. `shared.db.slurm_jobs`."""
    ensure_connected()
    doc = models.SlurmJob.objects(id=get_slurm_job_db_id(cluster_id, job_id)).first()
    return None if doc is None else to_slurm_job(doc)
