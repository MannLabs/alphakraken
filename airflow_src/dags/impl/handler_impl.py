"""Business logic for the "acquisition_handler" DAG."""

import logging
import re
from pathlib import Path

from airflow.models import TaskInstance
from common.keys import DagContext, DagParams, Dags, OpArgs, XComKeys
from common.settings import (
    DEFAULT_RAW_FILE_SIZE_IF_MAIN_FILE_MISSING,
    Timings,
    get_internal_backup_path,
)
from common.utils import get_env_variable, get_xcom, trigger_dag_run
from file_handling import copy_file, get_file_size
from raw_file_wrapper_factory import CopyPathProvider, RawFileWrapperFactory

from shared.db.interface import get_raw_file_by_id, update_raw_file
from shared.db.models import RawFileStatus
from shared.keys import (
    ALLOWED_CHARACTERS_IN_RAW_FILE_NAME,
    DDA_FLAG_IN_RAW_FILE_NAME,
    EnvVars,
)


def copy_raw_file(ti: TaskInstance, **kwargs) -> None:
    """Copy a raw file to the target location."""
    del ti  # unused
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]

    raw_file = get_raw_file_by_id(raw_file_id)

    update_raw_file(raw_file_id, new_status=RawFileStatus.COPYING)

    copy_wrapper = RawFileWrapperFactory.create_write_wrapper(
        raw_file=raw_file,
        path_provider=CopyPathProvider,
    )

    copied_files: dict[Path, tuple[float, str]] = {}
    for src_path, dst_path in copy_wrapper.get_files_to_copy().items():
        dst_size, dst_hash = copy_file(src_path, dst_path)
        copied_files[dst_path] = (dst_size, dst_hash)

    file_info = _get_file_info(copied_files)

    pool_base_path = Path(get_env_variable(EnvVars.POOL_BASE_PATH))
    backup_pool_folder = get_env_variable(EnvVars.BACKUP_POOL_FOLDER)
    backup_base_path = pool_base_path / backup_pool_folder

    # a bit hacky to get the file size once again, but it's a cheap operation and avoids complicate logic
    # TODO: in rare cases (manual intervention) this could yield to inconsistencies, change this!
    file_size = get_file_size(
        copy_wrapper.file_path_to_calculate_size(),
        DEFAULT_RAW_FILE_SIZE_IF_MAIN_FILE_MISSING,
    )
    update_raw_file(
        raw_file_id,
        new_status=RawFileStatus.COPYING_DONE,
        size=file_size,
        file_info=file_info,
        backup_base_path=str(backup_base_path),
    )


def _get_file_info(
    copied_files: dict[Path, tuple[float, str]],
) -> dict[str, tuple[float, str]]:
    """Map the paths of the copied files from the internal to their actual locations.

    e.g. from `/opt/airflow/mounts/backup/test1/2024_08/test_file_SA_P1_1.raw` -> `test1/2024_08/test_file_SA_P1_1.raw`
    """
    internal_backup_path = get_internal_backup_path()

    file_info: dict[str, tuple[float, str]] = {}
    for dst_path, file_size_and_hash in copied_files.items():
        rel_dst_path = dst_path.relative_to(internal_backup_path)
        file_info[str(rel_dst_path)] = file_size_and_hash

    return file_info


def start_file_mover(ti: TaskInstance, **kwargs) -> None:
    """Trigger the file_mover DAG for a specific raw file."""
    del ti  # unused
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]

    trigger_dag_run(
        Dags.FILE_MOVER,
        {
            DagParams.RAW_FILE_ID: raw_file_id,
        },
        # start only after some time to detect upstream false positive errors in detecting finished acquisitions
        time_delay_minutes=Timings.FILE_MOVE_DELAY_M,
    )


def _count_special_characters(raw_file_id: str) -> int:
    """Check if the raw file name contains special characters."""
    pattern = re.compile(ALLOWED_CHARACTERS_IN_RAW_FILE_NAME)
    return len(pattern.findall(raw_file_id))


def decide_processing(ti: TaskInstance, **kwargs) -> bool:
    """Decide whether to start the acquisition_processor DAG.

    Skip the downstream tasks if the raw file is not suitable for processing:
        - if the acquisition monitor has reported errors
        - if the raw file name contains the DDA flag
        - if the raw file name contains special characters
    """
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]

    if acquisition_monitor_errors := get_xcom(
        ti, XComKeys.ACQUISITION_MONITOR_ERRORS, []
    ):
        new_status = RawFileStatus.ACQUISITION_FAILED
        status_details = ";".join(acquisition_monitor_errors)
        logging.info(f"Acquisition monitor errors: {acquisition_monitor_errors}.")
    elif DDA_FLAG_IN_RAW_FILE_NAME in raw_file_id.lower():
        new_status = RawFileStatus.DONE_NOT_QUANTED
        status_details = "Filename contains 'dda'."
        logging.info(f"f{raw_file_id} contains 'dda'.")
    elif _count_special_characters(raw_file_id):
        new_status = RawFileStatus.DONE_NOT_QUANTED
        status_details = "Filename contains special characters."
        logging.info(f"{raw_file_id} contains special characters.")
    else:
        return True  # continue with downstream tasks

    # potential other checks:
    #  - file size to small -> Variable?

    logging.info("Skipping downstream tasks..")

    update_raw_file(
        raw_file_id,
        new_status=new_status,
        status_details=status_details,
    )

    return False  # skip downstream tasks


def start_acquisition_processor(ti: TaskInstance, **kwargs) -> None:
    """Trigger an acquisition_processor DAG run for specific raw files.

    Each raw file is added to the database first.
    Then, for each raw file, the project id is determined.
    Only for raw files that carry a project id, the acquisition_handler DAG is triggered.
    """
    del ti  # unused
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]

    dag_id_to_trigger = f"{Dags.ACQUISITON_HANDLER}.{instrument_id}"

    update_raw_file(raw_file_id, new_status=RawFileStatus.QUEUED_FOR_QUANTING)

    trigger_dag_run(
        dag_id_to_trigger,
        {
            DagParams.RAW_FILE_ID: raw_file_id,
        },
    )
