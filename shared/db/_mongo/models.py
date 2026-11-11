"""Database models for the MongoDB."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import ClassVar

from mongoengine import (
    DateTimeField,
    DictField,
    Document,
    DynamicDocument,
    IntField,
    ListField,
    QuerySet,
    ReferenceField,
    StringField,
)

from shared.domain.projects import (
    PROJECT_ID_MAX_LENGTH,
    PROJECT_ID_MIN_LENGTH,
    ProjectStatus,
)
from shared.domain.raw_files import InstrumentFileStatus
from shared.domain.settings import (
    SETTINGS_NAME_REGEX,
    SETTINGS_VERSION_SEPARATOR,
    SettingsStatus,
)
from shared.domain.slurm_jobs import SLURM_JOB_EXPIRY_S
from shared.domain.status import KrakenStatusEntities
from shared.domain.users import USER_EMAIL_REGEX, USER_INITIALS_REGEX, UserStatus
from shared.keys import (
    ALERT_SUPPRESSION_WILDCARD,
    DEFAULT_SCOPE,
    FALLBACK_PROJECT_ID,
    MetricsTypes,
    SoftwareTypes,
)


class RawFile(Document):
    """Schema for a raw file."""

    meta: ClassVar = {"strict": False}
    objects: ClassVar[QuerySet[RawFile]]

    # Unique identifier of the file. Either the raw file name or, in case of a collision,
    # the raw file name with a unique prefix.
    id = StringField(
        max_length=255, required=True, primary_key=True
    )  # max_length of file names on linux: 255

    # Unique prefix to indicate a collision. If None, no collision occurred.
    collision_flag = StringField(max_length=32, default=None)

    # Original name of the file. In case of collisions, this is not unique. Otherwise, equal to `id`.
    original_name = StringField(max_length=255, required=True)

    instrument_id = StringField(max_length=32)
    project_id = StringField(max_length=32, default=FALLBACK_PROJECT_ID)

    status = StringField(max_length=32)
    status_details = StringField(max_length=512)

    size = IntField(min_value=-1, max_value=int(1000 * 1024**3))  # unit: bytes

    backup_status = StringField(max_length=32, default=None)

    # `display_paths.backup` at copy time: records which backup file system holds the file
    backup_display_base_path = StringField(max_length=1024, default=None)

    # bucket_name or bucket_name/sub_path/ -> file_info paths are always relative to that
    s3_upload_path = StringField(max_length=1024, default=None)

    instrument_file_status = StringField(max_length=16, default=InstrumentFileStatus.NA)

    file_info = DictField()  # mapping of file paths (relative to the raw file folder, cf. path_layout) to tuples (size: int, hash: str) or (size: int, hash: str, etag: str).
    # When read from DB, the tuples are converted to lists.

    created_at = DateTimeField()  # when file was created
    finalized_at = DateTimeField()  # when acquisition was finished (mtime of main file)

    # audit fields
    created_at_ = DateTimeField(default=lambda: datetime.now(UTC))
    updated_at_ = DateTimeField(default=lambda: datetime.now(UTC))

    @property
    def has_project(self) -> bool:
        """Return whether the raw file has a project assigned or belongs to the fallback project."""
        return self.project_id != FALLBACK_PROJECT_ID


class Metrics(DynamicDocument):
    """Schema for metrics calculated for a raw file.

    Inheriting from `DynamicDocument` means that any parameter passed to the model will be added to the DB.
    cf. https://docs.mongoengine.org/guide/defining-documents.html#dynamic-document-schemas
    """

    objects: ClassVar[QuerySet[Metrics]]

    # https://docs.mongoengine.org/guide/defining-documents.html#reference-fields
    raw_file = ReferenceField(RawFile)

    settings_name = StringField(max_length=64, default="n/a")
    settings_version = IntField(min_value=1, default=1)

    # Type of metrics: e.g. "alphadia" (default), "custom", ..
    type = StringField(max_length=32, default=MetricsTypes.ALPHADIA)

    # relative to the `output` location, cf. path_layout
    relative_output_path = StringField(max_length=512)

    # `display_paths.output` at processing time: records which output file system holds the results
    output_display_base_path = StringField(max_length=1024, default=None)

    # audit fields
    created_at_ = DateTimeField(default=lambda: datetime.now(UTC))


class User(Document):
    """Schema for a user, used as owner of projects and settings.

    Not a login identity.
    """

    # uniqueness of initials is only checked in the interface, a race between concurrent writes is accepted
    meta: ClassVar = {"strict": False, "auto_create_index": False}
    objects: ClassVar[QuerySet[User]]

    email = StringField(
        required=True, primary_key=True, max_length=128, regex=USER_EMAIL_REGEX
    )
    initials = StringField(required=True, max_length=8, regex=USER_INITIALS_REGEX)
    # user id in the messenger (e.g. Slack member ID), to notify the user directly
    messenger_id = StringField(max_length=512)

    status = StringField(max_length=32, default=UserStatus.ACTIVE)

    created_at_ = DateTimeField(default=lambda: datetime.now(UTC))


class AlertIssue(Document):
    """An issue found by the monitoring, and when it was last notified.

    One document per issue key, kept as the notification memory across checks and restarts:
    an issue that disappears and comes back is subject to its cooldown like any other. The
    documents expire ALERT_ISSUE_EXPIRY_S after `first_seen` through a TTL index created by
    `misc/init-mongo.sh`, as the alerting user may not create indexes.
    """

    meta: ClassVar = {"strict": False, "auto_create_index": False}
    objects: ClassVar[QuerySet[AlertIssue]]

    # alert class name, e.g. "DiskSpaceAlert"
    alert = StringField(required=True, max_length=64)
    # stable per issue within an alert, cf. `Issue.key` in the monitoring
    key = StringField(required=True, max_length=512)

    first_seen = DateTimeField(required=True)
    # None: never notified
    last_notified = DateTimeField()


class AlertSuppression(Document):
    """Schema for a rule muting monitoring alerts; an issue is muted if all fields match.

    The match fields mirror those of `Issue` in the monitoring.
    """

    meta: ClassVar = {"strict": False}
    objects: ClassVar[QuerySet[AlertSuppression]]

    # alert class name, e.g. "PumpPressureHighAlert"
    alert = StringField(
        required=True, min_length=1, max_length=64, default=ALERT_SUPPRESSION_WILDCARD
    )
    # exact match on the instrument the issue relates to
    instrument_id = StringField(
        required=True, min_length=1, max_length=64, default=ALERT_SUPPRESSION_WILDCARD
    )
    # exact match on the issue identifier
    identifier = StringField(
        required=True, min_length=1, max_length=256, default=ALERT_SUPPRESSION_WILDCARD
    )
    # substring of the issue details
    details = StringField(
        required=True, min_length=1, max_length=256, default=ALERT_SUPPRESSION_WILDCARD
    )

    comment = StringField(required=True, max_length=512)
    # None: never expires
    expires_at = DateTimeField()
    created_by = StringField(max_length=64)
    created_at_ = DateTimeField(default=lambda: datetime.now(UTC))


class Project(Document):
    """Schema for a project."""

    meta: ClassVar = {"strict": False}
    objects: ClassVar[QuerySet[Project]]

    id = StringField(
        required=True,
        primary_key=True,
        min_length=PROJECT_ID_MIN_LENGTH,
        max_length=PROJECT_ID_MAX_LENGTH,
    )
    name = StringField(required=True, max_length=64)
    description = StringField(max_length=512)

    status = StringField(max_length=32, default=ProjectStatus.ACTIVE)

    owner = ReferenceField(User, default=None)

    created_at_ = DateTimeField(default=lambda: datetime.now(UTC))


class Settings(Document):
    """Schema for quanting settings."""

    meta: ClassVar = {
        "strict": False,
        "indexes": [{"fields": ["name", "version"], "unique": True}],
        "auto_create_index": False,  # to avoid adding write rights to read-only connections
    }
    objects: ClassVar[QuerySet[Settings]]

    # unique number for each settings
    number = IntField(
        min_value=1, unique=True, default=-1
    )  # default only needed for legacy data

    name = StringField(required=True, max_length=64, regex=SETTINGS_NAME_REGEX)
    version = IntField(min_value=1, default=1)
    description = StringField(max_length=512)

    fasta_file_name = StringField(required=False, max_length=128)
    speclib_file_name = StringField(required=False, max_length=128)

    config_file_name = StringField(required=False, max_length=128)
    config_params = StringField(required=False, max_length=2048)

    software_type = StringField(
        required=True,
        max_length=128,
        default=SoftwareTypes.ALPHADIA,  # TODO: remove, default is just for backwards compatibility
    )
    software = StringField(required=True, max_length=128)

    runner_name = StringField(
        required=True, max_length=64
    )  # a name in alphakraken.yaml `runners`

    metrics_type = StringField(required=True, max_length=128)

    # TODO: remove defaults after migration
    slurm_cpus_per_task = IntField(required=True, min_value=1, default=1)
    slurm_mem = StringField(required=True, max_length=16, default="")
    slurm_time = StringField(required=True, max_length=16, default="")
    num_threads = IntField(required=True, min_value=1, default=1)

    status = StringField(max_length=64, default=SettingsStatus.ACTIVE)

    owner = ReferenceField(User, default=None)

    created_at_ = DateTimeField(default=lambda: datetime.now(UTC))

    @property
    def unique_name(self) -> str:
        """Human-readable unique identifier, e.g. `fallback_v7`."""
        return f"{self.name}{SETTINGS_VERSION_SEPARATOR}{self.version}"


class ProjectSettings(Document):
    """Intermediate model for M:N Project-Settings assignments."""

    meta: ClassVar = {"strict": False, "auto_create_index": False}
    objects: ClassVar[QuerySet[ProjectSettings]]

    project = ReferenceField(Project, required=True)
    settings = ReferenceField(Settings, required=True)
    # "*", a vendor name or an instrument ID; an assignment applies if any scope matches and no excluded one does
    scopes = ListField(StringField(max_length=64), default=lambda: [DEFAULT_SCOPE])
    excluded_scopes = ListField(StringField(max_length=64), default=list)
    # substrings of the raw file ID; include = any matches (empty = all), exclude = none matches
    raw_file_id_filter = ListField(StringField(max_length=128), default=list)
    raw_file_id_exclude_filter = ListField(StringField(max_length=128), default=list)
    created_at_ = DateTimeField(default=lambda: datetime.now(UTC))


class KrakenStatus(Document):
    """Schema for the AlphaKraken status, representing health, status details, free space, entity type, and last update time."""

    meta: ClassVar = {"strict": False}
    objects: ClassVar[QuerySet[KrakenStatus]]

    id = StringField(max_length=64, required=True, primary_key=True)

    status = StringField(max_length=64, default="n/a")
    status_details = StringField(max_length=1024)

    free_space_gb = IntField(min_value=-1, default=-1)
    entity_type = StringField(max_length=16, default=KrakenStatusEntities.INSTRUMENT)

    updated_at_ = DateTimeField(default=lambda: datetime.now(UTC))


class SlurmJob(Document):
    """State of a Slurm job as last reported by `squeue`."""

    meta: ClassVar = {
        "strict": False,
        "indexes": [
            "cluster_id",
            {"fields": ["updated_at_"], "expireAfterSeconds": SLURM_JOB_EXPIRY_S},
        ],
    }
    objects: ClassVar[QuerySet[SlurmJob]]

    # "<cluster id>/<job id>", cf. `get_slurm_job_db_id`
    id = StringField(max_length=128, required=True, primary_key=True)

    # the SSH connection id prefix of the cluster
    cluster_id = StringField(max_length=64, required=True)

    state = StringField(max_length=32)

    updated_at_ = DateTimeField(default=lambda: datetime.now(UTC))
