"""Tests for the legacy metric keys migration script."""

import importlib.util
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

_SCRIPT = (
    Path(__file__).parents[3]
    / "_migrations/from_0.12.0/_migrate_legacy_metrics_keys.py"
)

# the migrations folder is no package, so the script is loaded by path
_spec = importlib.util.spec_from_file_location("migrate_legacy_metrics_keys", _SCRIPT)
migration: Any = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(migration)  # type: ignore[union-attr]


def test_get_changes_renames_legacy_keys() -> None:
    """Legacy keys get their current names."""
    doc = {"_id": 1, "msqc_proteins": 5, "raw:gradient_length_m": 30, "other": 1}

    assert migration._get_changes(doc) == (
        {"msqc_proteins": "proteins", "raw:gradient_length_m": "gradient_length"},
        [],
    )


def test_get_changes_drops_legacy_key_with_the_same_value() -> None:
    """A legacy key whose current name holds the same value is dropped."""
    doc = {"_id": 1, "msqc_proteins": 5, "proteins": 5}

    assert migration._get_changes(doc) == ({}, ["msqc_proteins"])


def test_get_changes_treats_nan_values_as_the_same() -> None:
    """NaN is not equal to itself, but two NaN values are the same metric value."""
    doc = {"_id": 1, "msqc_proteins": float("nan"), "proteins": float("nan")}

    assert migration._get_changes(doc) == ({}, ["msqc_proteins"])


def test_get_changes_keeps_legacy_key_with_another_value() -> None:
    """A legacy key whose current name holds another value is left alone."""
    doc = {"_id": 1, "msqc_proteins": 5, "proteins": 6}

    assert migration._get_changes(doc) == ({}, [])


def test_get_changes_compares_two_legacy_keys_of_one_current_name() -> None:
    """The second legacy key of one current name is dropped if it holds the same value as the first."""
    doc = {"_id": 1, "msqc_time_elapsed": 7, "quanting_time_elapsed": 7}

    assert migration._get_changes(doc) == (
        {"msqc_time_elapsed": "time_elapsed"},
        ["quanting_time_elapsed"],
    )


def test_migrate_renames_and_drops_in_one_update() -> None:
    """Renames and drops of a document are written together; nothing is written in a dry run."""
    collection = MagicMock()
    collection.find.return_value = [
        {"_id": 1, "msqc_a": 1, "msqc_b": 2, "b": 2},
        {"_id": 2, "c": 3},
    ]

    with (
        patch.object(migration, "connect_db"),
        patch.object(migration.Metrics, "_get_collection", return_value=collection),
    ):
        migration.migrate(dry_run=True)
        collection.update_one.assert_not_called()

        migration.migrate(dry_run=False)

    collection.update_one.assert_called_once_with(
        {"_id": 1}, {"$rename": {"msqc_a": "a"}, "$unset": {"msqc_b": ""}}
    )
