"""Module to handle interactions with the database.

Note: this module must not have any dependencies on the rest of the codebase.
"""

import logging
import re
from datetime import UTC, datetime, timedelta

from pymongo import UpdateOne

from shared.db.engine import connect_db
from shared.db.models import (
    Metrics,
    Project,
    ProjectSettings,
    RawFile,
    Settings,
    SlurmJob,
    User,
    get_slurm_job_db_id,
)
from shared.domain.projects import ProjectStatus
from shared.domain.raw_files import FileInfoItem, InstrumentFileStatus
from shared.domain.settings import SettingsStatus
from shared.domain.slurm_jobs import SlurmJobQueueStates
from shared.keys import DEFAULT_SCOPE

_NON_ACTIVE_PROJECT_STATUSES = [ProjectStatus.INACTIVE, ProjectStatus.DELETED]


def get_raw_files_by_names(
    raw_file_names: list[str], *, case_sensitive: bool = False
) -> list[RawFile]:
    """Get raw files from the database with the given original names, by default ignoring case."""
    logging.info(f"Getting from DB: {raw_file_names=} {case_sensitive=}")
    connect_db()
    names: list[str] | list[re.Pattern[str]] = (
        raw_file_names
        if case_sensitive
        else [
            re.compile(f"^{re.escape(name)}$", re.IGNORECASE) for name in raw_file_names
        ]
    )
    return list(RawFile.objects.filter(original_name__in=names))


def get_raw_file_by_id(raw_file_id: str) -> RawFile:
    """Get raw file from the database with the given id."""
    logging.info(f"Getting from DB: {raw_file_id=}")
    connect_db()
    return RawFile.objects(id=raw_file_id).first()


def get_purge_candidates_by_age(
    instrument_id: str,
    *,
    min_age_in_days: float,
) -> list[RawFile]:
    """Get raw files older than the given age in days for a given instrument that are not purged, renamed or disappeared, sorted 'oldest first'."""
    connect_db()

    youngest_created_at = datetime.now(tz=UTC) - timedelta(hours=min_age_in_days * 24)
    logging.info(f"Getting from DB: {instrument_id=} {youngest_created_at=}")

    return list(
        RawFile.objects.filter(
            instrument_id=instrument_id,
            created_at__lt=youngest_created_at,
            instrument_file_status__nin=[
                InstrumentFileStatus.PURGED,
                InstrumentFileStatus.RENAMED,
                InstrumentFileStatus.DISAPPEARED,
            ],
        ).order_by("created_at")
    )


def add_raw_file(  # noqa: PLR0913 too many arguments
    file_name: str,
    collision_flag: str | None = None,
    *,
    project_id: str | None,
    status: str,
    instrument_file_status: str,
    instrument_id: str,
    creation_ts: float,
) -> str:
    """Add a new raw file to the database.

    :param file_name: name of the file
    :param collision_flag: optional flag to indicate a collision
    :param project_id: project id
    :param status: status of the file
    :param instrument_file_status: status of the physical file on the instrument, see InstrumentFileStatus for possible values
    :param instrument_id: id of the acquiring instrument
    :param creation_ts: creation timestamp (unix)
    :return: the raw file id. This is either equal to the raw_file_name or has a collision flag prefixed
    """
    logging.info(
        f"Adding to DB: {file_name=} {collision_flag=} {project_id=} {status=} {instrument_id=} {creation_ts=}"
    )
    connect_db()

    id_ = file_name if collision_flag is None else f"{collision_flag}{file_name}"

    raw_file = RawFile(
        id=id_,
        collision_flag=collision_flag,
        original_name=file_name,
        project_id=project_id,
        instrument_id=instrument_id,
        status=status,
        instrument_file_status=instrument_file_status,
        created_at=datetime.fromtimestamp(creation_ts, UTC),
    )
    # this will fail if the file already exists
    raw_file.save(force_insert=True)

    return id_


def delete_raw_file(raw_file_id: str) -> None:
    """Remove raw file from the database."""
    logging.info(f"Removing from DB: {raw_file_id=}")
    connect_db()
    RawFile.objects(id=raw_file_id).delete()


# sentinel value to indicate that a parameter should not be updated
_NO_UPDATE = object()


def update_raw_file(  # noqa: PLR0913
    raw_file_id: str,
    *,
    new_status: str = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
    status_details: str | None = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
    size: float = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
    file_info: dict[str, FileInfoItem] = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
    backup_status: str = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
    backup_display_base_path: str = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
    s3_upload_path: str = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
    instrument_file_status: str = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
    finalized_at: datetime | None = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
    created_at: datetime = _NO_UPDATE,  # type: ignore[invalid-parameter-default]
) -> None:
    """Update parameters of DB entity of raw file with `raw_file_id`."""
    logging.info(
        f"Updating DB: {raw_file_id=} to {new_status=} {status_details=} {size=} {file_info=} {backup_status=} {backup_display_base_path=} {s3_upload_path=} {instrument_file_status=} {finalized_at=} {created_at=}"
    )
    connect_db()
    raw_file = RawFile.objects.with_id(raw_file_id)
    logging.info(f"Old DB state: {raw_file.status=} {raw_file.status_details=}")

    # prevent overwriting these fields with None if they are not given
    kwargs = {
        "status": new_status,
        "updated_at_": datetime.now(tz=UTC),
        "status_details": status_details,
        "size": size,
        "file_info": file_info,
        "backup_status": backup_status,
        "backup_display_base_path": backup_display_base_path,
        "s3_upload_path": s3_upload_path,
        "instrument_file_status": instrument_file_status,
        "finalized_at": finalized_at,
        "created_at": created_at,
    }

    raw_file.update(**{k: v for k, v in kwargs.items() if v != _NO_UPDATE})


def add_metrics_to_raw_file(  # noqa: PLR0913
    raw_file_id: str,
    *,
    metrics_type: str,
    metrics: dict,
    settings_name: str,
    settings_version: int,
    relative_output_path: str,
    output_display_base_path: str,
) -> None:
    """Add `metrics` to DB entry of `raw_file_id`."""
    logging.info(
        f"Adding to DB: {raw_file_id=} <- {metrics=} type={metrics_type} {settings_name=} {settings_version=} {relative_output_path=} {output_display_base_path=}"
    )
    connect_db()
    raw_file = RawFile.objects.get(id=raw_file_id)

    Metrics(
        raw_file=raw_file,
        type=metrics_type,
        settings_name=settings_name,
        settings_version=settings_version,
        relative_output_path=relative_output_path,
        output_display_base_path=output_display_base_path,
        **metrics,
    ).save()


def add_project(
    *, project_id: str, name: str, description: str, owner_email: str | None = None
) -> None:
    """Add a new project to the database."""
    logging.info(f"Adding to DB: {project_id=} {name=} {description=} {owner_email=}")
    connect_db()
    existing = Project.objects(id=project_id).first()
    if existing is not None and existing.status == ProjectStatus.DELETED:
        raise ValueError(
            f"Project id '{project_id}' was used by a deleted project. Please choose a different one."
        )
    project = Project(
        id=project_id,
        name=name,
        description=description,
        owner=_get_owner(owner_email),
    )
    # this will fail if the project id already exists
    project.save(force_insert=True)


def archive_project(project_id: str) -> None:
    """Archive a project by setting its status to INACTIVE."""
    connect_db()
    Project.objects(id=project_id).update_one(status=ProjectStatus.INACTIVE)
    logging.info(f"Archived project: {project_id=}")


def delete_project(project_id: str) -> None:
    """Soft-delete a project by setting its status to DELETED."""
    connect_db()
    if Project.objects.get(id=project_id).status == ProjectStatus.INACTIVE:
        raise ValueError(f"Cannot delete archived project '{project_id}'")
    Project.objects(id=project_id).update_one(status=ProjectStatus.DELETED)
    logging.info(f"Deleted project: {project_id=}")


def get_active_project_ids() -> list[str]:
    """Get the ids of all projects that are neither archived nor deleted."""
    connect_db()
    return [p.id for p in Project.objects(status=ProjectStatus.ACTIVE)]


def assign_settings_to_project(  # noqa: PLR0913
    project_id: str,
    settings_id: str,
    scopes: list[str] | None = None,
    excluded_scopes: list[str] | None = None,
    raw_file_id_filter: list[str] | None = None,
    raw_file_id_exclude_filter: list[str] | None = None,
) -> ProjectSettings:
    """Create a new project-settings assignment."""
    connect_db()
    project = Project.objects.get(id=project_id)
    if project.status in _NON_ACTIVE_PROJECT_STATUSES:
        raise ValueError(
            f"Cannot assign settings to archived or deleted project '{project_id}'"
        )
    settings = Settings.objects.get(id=settings_id)
    if settings.status == SettingsStatus.INACTIVE:
        raise ValueError(
            f"Cannot assign archived settings '{settings.name}' version {settings.version}"
        )

    # the default scope matches everything, so any other entry next to it is redundant
    scopes = [DEFAULT_SCOPE] if not scopes or DEFAULT_SCOPE in scopes else scopes
    excluded_scopes = excluded_scopes or []
    raw_file_id_filter = raw_file_id_filter or []
    raw_file_id_exclude_filter = raw_file_id_exclude_filter or []

    check_not_assigned_with_same_filters(
        project_id, settings, raw_file_id_filter, raw_file_id_exclude_filter
    )

    ps = ProjectSettings(
        project=project,
        settings=settings,
        # TODO: validate here, not only on the frontend: reject DEFAULT_SCOPE in excluded_scopes
        #  and scopes overlapping excluded_scopes, both silently match nothing
        scopes=scopes,
        excluded_scopes=excluded_scopes,
        raw_file_id_filter=raw_file_id_filter,
        raw_file_id_exclude_filter=raw_file_id_exclude_filter,
    )
    ps.save()
    logging.info(
        f"Created project-settings assignment: {project_id=} {settings.name=} {scopes=} "
        f"{excluded_scopes=} {raw_file_id_filter=} {raw_file_id_exclude_filter=}"
    )
    return ps


def check_not_assigned_with_same_filters(
    project_id: str,
    settings: Settings,
    raw_file_id_filter: list[str],
    raw_file_id_exclude_filter: list[str],
) -> None:
    """Raise if the settings are already assigned to the project with identical file name filters."""
    connect_db()
    for ps_existing in ProjectSettings.objects(project=project_id, settings=settings):
        if set(ps_existing.raw_file_id_filter) == set(raw_file_id_filter) and _lower(
            ps_existing.raw_file_id_exclude_filter
        ) == _lower(raw_file_id_exclude_filter):
            raise ValueError(
                f"Settings '{settings.name}' version {settings.version} already assigned "
                f"to project '{project_id}' with the same file name filters. "
                "Unlink the existing assignment and re-assign with the merged scopes."
            )


def _lower(filters: list[str]) -> set[str]:
    return {f.lower() for f in filters}


def unassign_settings_from_project(project_settings_id: str) -> None:
    """Remove a project-settings assignment by its ID."""
    connect_db()
    ps = ProjectSettings.objects.get(id=project_settings_id)
    if ps.project.status in _NON_ACTIVE_PROJECT_STATUSES:
        raise ValueError(
            f"Cannot unassign settings from archived or deleted project '{ps.project.id}'"
        )
    ps.delete()
    logging.info(f"Removed project-settings assignment: {project_settings_id=}")


def get_project_settings(project_id: str) -> list[ProjectSettings]:
    """Get all project-settings assignments for a project."""
    connect_db()
    return list(ProjectSettings.objects(project=project_id).order_by("created_at_"))


def _get_owner(owner_email: str | None) -> User | None:
    """Get the user to set as owner, raises if it does not exist."""
    return User.objects.get(email=owner_email) if owner_email else None


def get_settings_by_id(settings_id: str) -> Settings:
    """Get a settings entry by its ID."""
    connect_db()
    return Settings.objects.get(id=settings_id)


def get_all_settings(*, include_archived: bool = False) -> list[Settings]:
    """Get all non-deleted settings from the database."""
    connect_db()
    if include_archived:
        return list(
            Settings.objects(status__ne=SettingsStatus.DELETED).order_by("-created_at_")
        )
    return list(Settings.objects(status=SettingsStatus.ACTIVE).order_by("-created_at_"))


def get_latest_active_settings_by_name(name: str) -> Settings | None:
    """Get the latest active version of settings with the given name."""
    connect_db()
    return (
        Settings.objects(name=name, status=SettingsStatus.ACTIVE)
        .order_by("-version")
        .first()
    )


def update_slurm_jobs(cluster_id: str, states: dict[str, str]) -> None:
    """Store the `states` (job id -> state) of all jobs in the queue of the cluster `cluster_id`.

    Jobs of the cluster that are stored but not in `states` get the state `LEFT_QUEUE`.
    """
    connect_db()
    now = datetime.now(tz=UTC)
    db_ids = [get_slurm_job_db_id(cluster_id, job_id) for job_id in states]

    if states:
        # one round trip for all jobs
        SlurmJob._get_collection().bulk_write(  # noqa: SLF001
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

    SlurmJob.objects(
        cluster_id=cluster_id,
        state__ne=SlurmJobQueueStates.LEFT_QUEUE,
        id__nin=db_ids,
    ).update(set__state=SlurmJobQueueStates.LEFT_QUEUE, set__updated_at_=now)


def get_slurm_job(cluster_id: str, job_id: str) -> SlurmJob | None:
    """Get the Slurm job `job_id` of the cluster `cluster_id`, None if it was never seen in the queue."""
    connect_db()

    return SlurmJob.objects(id=get_slurm_job_db_id(cluster_id, job_id)).first()
