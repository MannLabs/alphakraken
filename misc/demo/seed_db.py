"""Seed the demo database with the project and the quanting settings the demo needs.

Meant to be piped into the running webapp container, which has the Mongo credentials and the write
rights for exactly these three collections:

    ./compose.sh --profile demo exec -T webapp python - < misc/demo/seed_db.py

Idempotent: anything that already exists is left alone.
"""

from shared.db.interface import (
    add_project,
    assign_settings_to_project,
    create_settings,
    get_all_project_ids,
    get_latest_active_settings_by_name,
    get_project_settings,
)
from shared.keys import JobEngines, MetricsTypes, SoftwareTypes

# must appear as an underscore-separated token in the raw file names, cf. download_raw_files.py
PROJECT_ID = "ADIAMA"
SETTINGS_NAME = "demo_msqc"

# The `docker` job engine takes a container image instead of an executable, built by setup_demo.sh.
MSQC_IMAGE = "alphakraken-msqc"
# placeholders are resolved by the processor, cf. _substitute_config_params()
MSQC_CONFIG_PARAMS = "RAW_FILE_PATH OUTPUT_PATH NUM_THREADS"

NUM_THREADS = 2
MEMORY = "8G"
# ignored by the `docker` job engine, which has no wall clock limit
TIME_LIMIT = "01:00:00"


def _seed_project() -> None:
    if PROJECT_ID in get_all_project_ids():
        print(f"Project {PROJECT_ID} already exists.")  # noqa: T201
        return

    add_project(
        project_id=PROJECT_ID,
        name="AlphaKraken demo",
        description="Self-contained demo, cf. misc/demo/README.md",
    )
    print(f"Created project {PROJECT_ID}.")  # noqa: T201


def _seed_settings() -> str:
    if (settings := get_latest_active_settings_by_name(SETTINGS_NAME)) is not None:
        print(f"Settings {SETTINGS_NAME} already exist.")  # noqa: T201
        return str(settings.id)  # type: ignore[unresolved-attribute]

    settings = create_settings(
        name=SETTINGS_NAME,
        description="msqc-extractor in a container on the demo host",
        # the `docker` job engine is only supported for the 'custom' software type
        software_type=SoftwareTypes.CUSTOM,
        software=MSQC_IMAGE,
        job_engine=JobEngines.DOCKER,
        metrics_type=MetricsTypes.MSQC,
        config_params=MSQC_CONFIG_PARAMS,
        # the docker engine turns these into the container's cpu and memory limits
        slurm_cpus_per_task=NUM_THREADS,
        slurm_mem=MEMORY,
        slurm_time=TIME_LIMIT,
        num_threads=NUM_THREADS,
    )
    print(f"Created settings {SETTINGS_NAME}.")  # noqa: T201
    return str(settings.id)  # type: ignore[unresolved-attribute]


def _seed_assignment(settings_id: str) -> None:
    if any(
        ps.settings.name == SETTINGS_NAME for ps in get_project_settings(PROJECT_ID)
    ):
        print(f"Settings {SETTINGS_NAME} already assigned to {PROJECT_ID}.")  # noqa: T201
        return

    assign_settings_to_project(project_id=PROJECT_ID, settings_id=settings_id)
    print(f"Assigned settings {SETTINGS_NAME} to project {PROJECT_ID}.")  # noqa: T201


if __name__ == "__main__":
    _seed_project()
    _seed_assignment(_seed_settings())
