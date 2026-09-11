"""Tests for the absolute -> relative paths migration script."""

import importlib.util
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

_SCRIPT = (
    Path(__file__).parents[3] / "_migrations/from_0.10.0/_migrate_paths_to_relative.py"
)

# the migrations folder is no package, so the script is loaded by path
_spec = importlib.util.spec_from_file_location("migrate_paths_to_relative", _SCRIPT)
migration: Any = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(migration)  # type: ignore[union-attr]


def test_strip_base_path_uses_the_matching_base_and_tolerates_a_trailing_slash() -> (
    None
):
    """Test that the path is made relative to the base it lies below."""
    assert (
        migration._strip_base_path(
            "/fs/out/P1/out_f.raw/alphadia", ["/other/", "/fs/out/"]
        )
        == "P1/out_f.raw/alphadia"
    )
    assert migration._strip_base_path("/fs/out/P1", ["/fs/output"]) is None
    assert migration._strip_base_path("/fs/output", ["/fs/output"]) is None


def test_migrate_metrics_rewrites_matching_documents_and_skips_the_rest() -> None:
    """Test that only documents below a known base path are rewritten, the rest is reported."""
    collection = MagicMock()
    collection.find.return_value = [
        {"_id": 1, "output_path": "/fs/out/P1/out_f.raw/alphadia"},
        {"_id": 2, "output_path": "/elsewhere/P1/out_f.raw/alphadia"},
    ]

    # when
    counts = migration._migrate_metrics(collection, ["/fs/out"], dry_run=False)

    # then
    assert counts == {"migrated": 1, "skipped": 1}
    collection.find.assert_called_once_with({"output_path": {"$exists": True}})
    collection.update_one.assert_called_once_with(
        {"_id": 1},
        {
            "$set": {"relative_output_path": "P1/out_f.raw/alphadia"},
            "$unset": {"output_path": ""},
        },
    )


def test_dry_run_writes_nothing() -> None:
    """Test that a dry run counts but leaves both collections untouched."""
    metrics = MagicMock()
    metrics.find.return_value = [{"_id": 1, "output_path": "/fs/out/P1"}]
    raw_files = MagicMock()
    raw_files.count_documents.return_value = 3

    # when
    counts = migration._migrate_metrics(metrics, ["/fs/out"], dry_run=True)
    removed = migration._migrate_raw_files(raw_files, dry_run=True)

    # then
    assert counts == {"migrated": 1}
    assert removed == 3
    metrics.update_one.assert_not_called()
    raw_files.update_many.assert_not_called()


def test_migrate_raw_files_unsets_the_field() -> None:
    """Test that the backup base path is removed from every document carrying it."""
    raw_files = MagicMock()
    raw_files.count_documents.return_value = 2

    # when
    removed = migration._migrate_raw_files(raw_files, dry_run=False)

    # then
    assert removed == 2
    raw_files.update_many.assert_called_once_with(
        {"backup_base_path": {"$exists": True}}, {"$unset": {"backup_base_path": ""}}
    )
