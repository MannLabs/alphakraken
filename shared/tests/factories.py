"""Factories for domain objects, with plausible defaults that tests override per field."""

from datetime import UTC, datetime
from typing import Any

from shared.domain.alerts import AlertIssue, AlertSuppression
from shared.domain.metrics import Metrics
from shared.domain.projects import Project, ProjectSettings, ProjectStatus
from shared.domain.raw_files import InstrumentFileStatus, RawFile, RawFileStatus
from shared.domain.settings import Settings, SettingsStatus
from shared.domain.status import KrakenStatus, KrakenStatusEntities, KrakenStatusValues
from shared.domain.users import User, UserStatus
from shared.keys import (
    ALERT_SUPPRESSION_WILDCARD,
    DEFAULT_SCOPE,
    FALLBACK_PROJECT_ID,
    MetricsTypes,
)

SOME_TIME = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)


def raw_file(**overrides: Any) -> RawFile:
    """Return a raw file in DONE status."""
    values: dict[str, Any] = {
        "id": "file.raw",
        "collision_flag": None,
        "original_name": "file.raw",
        "instrument_id": "inst1",
        "project_id": FALLBACK_PROJECT_ID,
        "status": RawFileStatus.DONE,
        "status_details": None,
        "size": 1024,
        "backup_status": None,
        "backup_display_base_path": None,
        "s3_upload_path": None,
        "instrument_file_status": InstrumentFileStatus.NA,
        "file_info": None,
        "created_at": SOME_TIME,
        "finalized_at": None,
        "created_at_": SOME_TIME,
        "updated_at_": SOME_TIME,
    }
    return RawFile(**{**values, **overrides})


def metrics(**overrides: Any) -> Metrics:
    """Return alphadia metrics of `file.raw` with one value."""
    values: dict[str, Any] = {
        "raw_file_id": "file.raw",
        "type": MetricsTypes.ALPHADIA,
        "settings_name": "fallback",
        "settings_version": 1,
        "relative_output_path": "P1/out_file.raw/fallback_v1",
        "output_display_base_path": "/output",
        "created_at": SOME_TIME,
        "values": {"proteins": 5000},
    }
    return Metrics(**{**values, **overrides})


def kraken_status(**overrides: Any) -> KrakenStatus:
    """Return the status of a healthy instrument."""
    values: dict[str, Any] = {
        "id": "inst1",
        "status": KrakenStatusValues.OK,
        "status_details": None,
        "free_space_gb": 100,
        "entity_type": KrakenStatusEntities.INSTRUMENT,
        "updated_at_": SOME_TIME,
    }
    return KrakenStatus(**{**values, **overrides})


def alert_issue(**overrides: Any) -> AlertIssue:
    """Return an alert issue that was never notified."""
    values: dict[str, Any] = {
        "id": "issue1",
        "alert": "DiskSpaceAlert",
        "key": "inst1",
        "first_seen": SOME_TIME,
        "last_notified": None,
    }
    return AlertIssue(**{**values, **overrides})


def alert_suppression(**overrides: Any) -> AlertSuppression:
    """Return an alert suppression that matches everything and never expires."""
    values: dict[str, Any] = {
        "id": "suppression1",
        "alert": ALERT_SUPPRESSION_WILDCARD,
        "instrument_id": ALERT_SUPPRESSION_WILDCARD,
        "identifier": ALERT_SUPPRESSION_WILDCARD,
        "details": ALERT_SUPPRESSION_WILDCARD,
        "comment": "comment",
        "expires_at": None,
        "created_by": None,
        "created_at_": SOME_TIME,
    }
    return AlertSuppression(**{**values, **overrides})


def user(**overrides: Any) -> User:
    """Return an active user with a messenger id."""
    values: dict[str, Any] = {
        "email": "masc@example.org",
        "initials": "MaSc",
        "messenger_id": "U_MASC",
        "status": UserStatus.ACTIVE,
        "created_at_": SOME_TIME,
    }
    return User(**{**values, **overrides})


def project(**overrides: Any) -> Project:
    """Return an active project without owner."""
    values: dict[str, Any] = {
        "id": "P1",
        "name": "Project 1",
        "description": "desc",
        "status": ProjectStatus.ACTIVE,
        "owner_email": None,
        "created_at_": SOME_TIME,
    }
    return Project(**{**values, **overrides})


def settings(**overrides: Any) -> Settings:
    """Return active alphadia settings `fallback_v7`."""
    values: dict[str, Any] = {
        "id": "fallback_v7",
        "number": 1,
        "name": "fallback",
        "version": 7,
        "description": None,
        "fasta_file_name": None,
        "speclib_file_name": None,
        "config_file_name": None,
        "config_params": None,
        "software_type": "alphadia",
        "software": "alphadia-1.0",
        "runner_name": "slurm",
        "metrics_type": "alphadia",
        "slurm_cpus_per_task": 1,
        "slurm_mem": "",
        "slurm_time": "",
        "num_threads": 1,
        "status": SettingsStatus.ACTIVE,
        "owner_email": None,
        "created_at_": SOME_TIME,
    }
    return Settings(**{**values, **overrides})


def project_settings(**overrides: Any) -> ProjectSettings:
    """Return the assignment of `fallback_v7` to project `P1` for all instruments and files."""
    values: dict[str, Any] = {
        "id": "assignment1",
        "project_id": "P1",
        "settings": settings(),
        "scopes": [DEFAULT_SCOPE],
        "excluded_scopes": [],
        "raw_file_id_filter": [],
        "raw_file_id_exclude_filter": [],
        "created_at_": SOME_TIME,
    }
    return ProjectSettings(**{**values, **overrides})
