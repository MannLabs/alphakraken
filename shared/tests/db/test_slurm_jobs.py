"""Tests for the slurm jobs API and its Mongo backend."""

from collections.abc import Iterator
from datetime import UTC, datetime
from unittest.mock import MagicMock, patch

import pytest
from pymongo import UpdateOne

from shared.db import slurm_jobs
from shared.db._mongo import models
from shared.domain.slurm_jobs import SlurmJob, SlurmJobQueueStates

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


@pytest.fixture
def mock_slurm_job() -> Iterator[MagicMock]:
    """Patch the SlurmJob model and the clock of the backend."""
    with (
        patch("shared.db._mongo.slurm_jobs.ensure_connected"),
        patch("shared.db._mongo.slurm_jobs.datetime") as mock_datetime,
        patch("shared.db._mongo.slurm_jobs.models") as mock_models,
    ):
        mock_datetime.now.return_value = NOW
        yield mock_models.SlurmJob


def test_upsert_states_upserts_queued_and_marks_left_jobs(
    mock_slurm_job: MagicMock,
) -> None:
    """Queued jobs are upserted in one round trip, all other jobs of the cluster are marked as left."""
    slurm_jobs.upsert_states("cluster1", {"1": "RUNNING", "2": "PENDING"})

    mock_slurm_job._get_collection.return_value.bulk_write.assert_called_once_with(
        [
            UpdateOne(
                {"_id": "cluster1/1"},
                {
                    "$set": {
                        "cluster_id": "cluster1",
                        "state": "RUNNING",
                        "updated_at_": NOW,
                    }
                },
                upsert=True,
            ),
            UpdateOne(
                {"_id": "cluster1/2"},
                {
                    "$set": {
                        "cluster_id": "cluster1",
                        "state": "PENDING",
                        "updated_at_": NOW,
                    }
                },
                upsert=True,
            ),
        ],
        ordered=False,
    )
    mock_slurm_job.objects.assert_called_once_with(
        cluster_id="cluster1",
        state__ne=SlurmJobQueueStates.LEFT_QUEUE,
        id__nin=["cluster1/1", "cluster1/2"],
    )
    mock_slurm_job.objects.return_value.update.assert_called_once_with(
        set__state=SlurmJobQueueStates.LEFT_QUEUE, set__updated_at_=NOW
    )


def test_upsert_states_empty_queue_marks_all_left(mock_slurm_job: MagicMock) -> None:
    """With an empty queue nothing is upserted and all jobs of the cluster are marked as left."""
    slurm_jobs.upsert_states("cluster1", {})

    mock_slurm_job._get_collection.assert_not_called()
    mock_slurm_job.objects.assert_called_once_with(
        cluster_id="cluster1", state__ne=SlurmJobQueueStates.LEFT_QUEUE, id__nin=[]
    )


def test_get(mock_slurm_job: MagicMock) -> None:
    """A job is looked up by its cluster-qualified id and returned with a UTC-aware datetime."""
    mock_slurm_job.objects.return_value.first.return_value = models.SlurmJob(
        id="cluster1/1",
        cluster_id="cluster1",
        state="RUNNING",
        updated_at_=NOW.replace(tzinfo=None),
    )

    assert slurm_jobs.get("cluster1", "1") == SlurmJob(
        id="cluster1/1", cluster_id="cluster1", state="RUNNING", updated_at_=NOW
    )
    mock_slurm_job.objects.assert_called_once_with(id="cluster1/1")


def test_get_job_never_seen(mock_slurm_job: MagicMock) -> None:
    """A job that was never in the polled queue is None."""
    mock_slurm_job.objects.return_value.first.return_value = None

    assert slurm_jobs.get("cluster1", "9") is None
