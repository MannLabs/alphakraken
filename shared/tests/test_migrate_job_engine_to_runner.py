"""Tests for the job_engine -> runner_name migration script."""

import importlib.util
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call

_SCRIPT = (
    Path(__file__).parents[1]
    / "_migrations/from_1.0.0/_migrate_job_engine_to_runner.py"
)

# the migrations folder is no package, so the script is loaded by path
_spec = importlib.util.spec_from_file_location("migrate_job_engine_to_runner", _SCRIPT)
migration: Any = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(migration)  # type: ignore[union-attr]


def _collection(docs: list[dict]) -> MagicMock:
    """Get a fake pymongo collection yielding the given documents."""
    collection = MagicMock()
    collection.find.return_value.sort.return_value = docs
    return collection


def test_migrates_each_legacy_document_once_and_skips_migrated_ones() -> None:
    """Test that only documents with `job_engine` and without `runner_name` are rewritten."""
    collection = _collection(
        [
            {"_id": 1, "name": "a", "version": 1, "job_engine": "slurm"},
            {"_id": 2, "name": "b", "version": 1, "runner_name": "slurm"},
            {"_id": 3, "name": "c", "version": 2, "job_engine": "file_based"},
        ]
    )

    # when
    target_names = migration._migrate_collection(collection, dry_run=False)

    assert target_names == {"slurm": 1, "file_based": 1}
    assert collection.update_one.call_args_list == [
        call(
            {"_id": 1},
            {"$set": {"runner_name": "slurm"}, "$unset": {"job_engine": ""}},
        ),
        call(
            {"_id": 3},
            {"$set": {"runner_name": "file_based"}, "$unset": {"job_engine": ""}},
        ),
    ]


def test_dry_run_reports_but_writes_nothing() -> None:
    """Test that a dry run yields the same summary without touching the collection."""
    collection = _collection(
        [{"_id": 1, "name": "a", "version": 1, "job_engine": "slurm"}]
    )

    # when
    target_names = migration._migrate_collection(collection, dry_run=True)

    assert target_names == {"slurm": 1}
    collection.update_one.assert_not_called()


def test_documents_without_either_field_are_skipped() -> None:
    """Test that a document predating the 0.8.0 backfill is left alone."""
    collection = _collection([{"_id": 1, "name": "a", "version": 1}])

    # when
    target_names = migration._migrate_collection(collection, dry_run=False)

    assert target_names == {}
    collection.update_one.assert_not_called()
