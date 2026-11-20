"""The data the integration tests of the reads rely on, written through `shared.db`."""

from datetime import UTC, datetime

from shared.db import metrics, project_settings, projects, raw_files, settings
from shared.domain.raw_files import FileInfoItem
from shared.keys import FALLBACK_PROJECT_ID

ACTIVE_PROJECT_ID = "SEEDACTIVE"
INACTIVE_PROJECT_ID = "SEEDINACTIVE"
# the only instrument of the `_test_` yaml, of type thermo
YAML_INSTRUMENT_ID = "_test1_"
OTHER_INSTRUMENT_ID = "tims1"

BACKUP_DISPLAY_BASE_PATH = "/fs/seed/backup"
OUTPUT_DISPLAY_BASE_PATH = "/fs/seed/output"
METRICS_SETTINGS_NAME = "seed"
METRICS_SETTINGS_VERSION = 1


def _create_settings(
    name: str, software_type: str, speclib_file_name: str | None = None
) -> str:
    """Create the next version of the settings and return its id."""
    return settings.create(
        name=name,
        software_type=software_type,
        software=f"{software_type}-1.0.0",
        runner_name="slurm",
        metrics_type=software_type,
        speclib_file_name=speclib_file_name,
    ).id


def _add_raw_file(  # noqa: PLR0913
    file_name: str,
    *,
    instrument_id: str,
    project_id: str,
    status: str,
    day: int,
    file_info: dict[str, FileInfoItem] | None = None,
    metrics_types: tuple[str, ...] = (),
) -> None:
    raw_file_id = raw_files.add(
        file_name,
        project_id=project_id,
        status=status,
        instrument_file_status="initial",
        instrument_id=instrument_id,
        creation_ts=datetime(2026, 1, day, 9, tzinfo=UTC).timestamp(),
    )
    raw_files.update(
        raw_file_id,
        backup_status="copying_done",
        backup_display_base_path=BACKUP_DISPLAY_BASE_PATH,
    )
    if file_info:
        raw_files.update(raw_file_id, file_info=file_info)
    for metrics_type in metrics_types:
        metrics.add(
            raw_file_id,
            metrics_type=metrics_type,
            values={"proteins": 2345},
            settings_name=METRICS_SETTINGS_NAME,
            settings_version=METRICS_SETTINGS_VERSION,
            relative_output_path=f"{project_id}/out_{raw_file_id}/{metrics_type}",
            output_display_base_path=OUTPUT_DISPLAY_BASE_PATH,
        )


def seed() -> None:
    """Fill an empty database: active and archived projects and settings, assignments, raw files with metrics."""
    alphadia_v1 = _create_settings(
        "seed_alphadia", "alphadia", speclib_file_name="speclib.hdf"
    )
    alphadia_v2 = _create_settings(
        "seed_alphadia", "alphadia", speclib_file_name="speclib.hdf"
    )
    msqc_v1 = _create_settings("seed_msqc", "msqc")

    # in a deployment, misc/init-mongo.sh creates the fallback project
    projects.add(project_id=FALLBACK_PROJECT_ID, name="Fallback", allow_fallback=True)
    projects.add(
        project_id=ACTIVE_PROJECT_ID, name="Active project", allow_fallback=True
    )
    projects.add(
        project_id=INACTIVE_PROJECT_ID, name="Inactive project", allow_fallback=True
    )

    for project_id, settings_id, scopes, fields in (
        (FALLBACK_PROJECT_ID, msqc_v1, ["*"], {}),
        (FALLBACK_PROJECT_ID, alphadia_v2, ["thermo"], {"excluded_scopes": ["bruker"]}),
        (
            FALLBACK_PROJECT_ID,
            alphadia_v2,
            ["sciex"],
            {"raw_file_id_exclude_filter": ["_blank"]},
        ),
        (
            ACTIVE_PROJECT_ID,
            alphadia_v2,
            [YAML_INSTRUMENT_ID],
            {"raw_file_id_filter": ["_HLA_"]},
        ),
        (ACTIVE_PROJECT_ID, alphadia_v1, ["bruker"], {}),
        (INACTIVE_PROJECT_ID, msqc_v1, ["*"], {}),
    ):
        project_settings.assign(
            project_id, settings_id, allow_fallback=True, scopes=scopes, **fields
        )

    # after the assignments, which need active projects and settings
    settings.archive(alphadia_v1)
    projects.archive(INACTIVE_PROJECT_ID)

    _add_raw_file(
        "20260105_test1_HLA_A1.raw",
        instrument_id=YAML_INSTRUMENT_ID,
        project_id=ACTIVE_PROJECT_ID,
        status="done",
        day=5,
        file_info={
            "20260105_test1_HLA_A1.raw": (1000000, "0123456789abcdef0123456789abcdef")
        },
        metrics_types=("msqc", "alphadia"),
    )
    _add_raw_file(
        "20260106_tims1_B2.d",
        instrument_id=OTHER_INSTRUMENT_ID,
        project_id=FALLBACK_PROJECT_ID,
        status="done",
        day=6,
        file_info={
            "20260106_tims1_B2.d/analysis.tdf": (
                1500000,
                "fedcba9876543210fedcba9876543210",
            ),
            "20260106_tims1_B2.d/analysis.tdf_bin": (
                500000,
                "00112233445566778899aabbccddeeff",
            ),
        },
        # two of a type: only the latest is returned
        metrics_types=("diann", "diann"),
    )
    _add_raw_file(
        "20260107_test1_C3.raw",
        instrument_id=YAML_INSTRUMENT_ID,
        project_id=FALLBACK_PROJECT_ID,
        status="quanting_failed",
        day=7,
    )
    _add_raw_file(
        "20260108_tims1_D4.d",
        instrument_id=OTHER_INSTRUMENT_ID,
        project_id=FALLBACK_PROJECT_ID,
        status="monitoring_acquisition",
        day=8,
    )
