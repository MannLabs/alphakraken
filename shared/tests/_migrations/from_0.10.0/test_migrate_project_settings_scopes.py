"""Tests for the ProjectSettings scopes migration script."""

import importlib.util
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, call

import pytest

_SCRIPT = (
    Path(__file__).parents[3]
    / "_migrations/from_0.10.0/_migrate_project_settings_scopes.py"
)

# the migrations folder is no package, so the script is loaded by path
_spec = importlib.util.spec_from_file_location(
    "migrate_project_settings_scopes", _SCRIPT
)
migration: Any = importlib.util.module_from_spec(_spec)  # type: ignore[arg-type]
_spec.loader.exec_module(migration)  # type: ignore[union-attr]


def _collection(docs: list[dict]) -> MagicMock:
    """Get a fake pymongo collection yielding the given documents."""
    collection = MagicMock()
    collection.find.return_value.sort.return_value = docs
    return collection


def test_migrates_each_legacy_document_and_skips_migrated_ones() -> None:
    """Test that only documents without `scopes` are rewritten, with fields mapped to lists."""
    collection = _collection(
        [
            {"_id": 1, "project": "P1", "settings": "s1", "scope": "*"},
            {
                "_id": 2,
                "project": "P1",
                "settings": "s2",
                "scope": "thermo",
                "excluded": ["instr1"],
                "raw_file_id_filter": "plasma",
            },
            {"_id": 3, "project": "P1", "settings": "s3", "scopes": ["*"]},
            {
                "_id": 4,
                "project": "P1",
                "settings": "s4",
                "scope": "*",
                "raw_file_id_filter": "",
            },
        ]
    )

    # when
    legacy = migration._migrate_collection(collection, dry_run=False)

    assert [doc["_id"] for doc in legacy] == [1, 2, 4]
    assert collection.update_one.call_args_list == [
        call(
            {"_id": 1},
            {
                "$set": {
                    "scopes": ["*"],
                    "excluded_scopes": [],
                    "raw_file_id_filter": [],
                    "raw_file_id_exclude_filter": [],
                },
                "$unset": {"scope": "", "excluded": ""},
            },
        ),
        call(
            {"_id": 2},
            {
                "$set": {
                    "scopes": ["thermo"],
                    "excluded_scopes": ["instr1"],
                    "raw_file_id_filter": ["plasma"],
                    "raw_file_id_exclude_filter": [],
                },
                "$unset": {"scope": "", "excluded": ""},
            },
        ),
        call(
            {"_id": 4},
            {
                "$set": {
                    "scopes": ["*"],
                    "excluded_scopes": [],
                    "raw_file_id_filter": [],
                    "raw_file_id_exclude_filter": [],
                },
                "$unset": {"scope": "", "excluded": ""},
            },
        ),
    ]


def test_dry_run_writes_nothing() -> None:
    """Test that a dry run returns the legacy documents without touching the collection."""
    collection = _collection(
        [{"_id": 1, "project": "P1", "settings": "s1", "scope": "*"}]
    )

    # when
    legacy = migration._migrate_collection(collection, dry_run=True)

    assert len(legacy) == 1
    collection.update_one.assert_not_called()


def test_malformed_document_aborts_before_the_first_write() -> None:
    """Test that a legacy document without a string scope stops the migration before any write."""
    collection = _collection(
        [
            {"_id": 1, "project": "P1", "settings": "s1", "scope": "*"},
            {"_id": 2, "project": "P1", "settings": "s2", "scope": ["*"]},
        ]
    )

    with pytest.raises(TypeError, match="`scope` missing or not a str"):
        migration._migrate_collection(collection, dry_run=False)

    collection.update_one.assert_not_called()


# --- co-firing report

_SETTINGS = {
    "a": {"name": "a", "version": 1, "software_type": "alphadia"},
    "b": {"name": "b", "version": 2, "software_type": "alphadia"},
    "c": {"name": "c", "version": 1, "software_type": "custom"},
}
_INSTRUMENTS = {"instr1": "thermo", "instr2": "bruker"}


def test_cofiring_report_lists_previously_overridden_assignments() -> None:
    """Test that the vendor assignment that used to override '*' is reported as newly firing."""
    docs = [
        {"_id": 1, "project": "P1", "settings": "a", "scope": "*"},
        {"_id": 2, "project": "P1", "settings": "b", "scope": "thermo"},
        {"_id": 3, "project": "P1", "settings": "c", "scope": "*"},
    ]

    report = migration.build_cofiring_report(docs, _SETTINGS, _INSTRUMENTS)

    # on instr1, 'b' (thermo) won over 'a' ('*') before; now 'a' fires in addition
    assert report == {("P1", "instr1"): ["a v1 (alphadia, scope='*')"]}


def test_cofiring_report_respects_exclusions_and_partitions() -> None:
    """Test that mutually exclusive vendor scopes and excluded instruments report nothing."""
    docs = [
        {"_id": 1, "project": "P1", "settings": "a", "scope": "thermo"},
        {"_id": 2, "project": "P1", "settings": "b", "scope": "bruker"},
        {
            "_id": 3,
            "project": "P2",
            "settings": "a",
            "scope": "*",
            "excluded": ["instr1"],
        },
        {"_id": 4, "project": "P2", "settings": "b", "scope": "instr1"},
    ]

    report = migration.build_cofiring_report(docs, _SETTINGS, _INSTRUMENTS)

    assert report == {}


def test_cofiring_report_same_level_first_wins() -> None:
    """Test that of two '*' assignments of one software_type, the second is the newly firing one."""
    docs = [
        {"_id": 1, "project": "P1", "settings": "a", "scope": "*"},
        {
            "_id": 2,
            "project": "P1",
            "settings": "b",
            "scope": "*",
            "raw_file_id_filter": "x",
        },
    ]

    report = migration.build_cofiring_report(docs, _SETTINGS, {"instr1": "thermo"})

    assert report == {("P1", "instr1"): ["b v2 (alphadia, scope='*', filter='x')"]}
