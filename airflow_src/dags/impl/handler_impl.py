"""Business logic for the "acquisition_handler" DAG."""

import logging
import re
from pathlib import Path

from airflow.exceptions import AirflowFailException
from airflow.models import TaskInstance
from common.keys import (
    DAG_DELIMITER,
    AcquisitionMonitorErrors,
    AirflowVars,
    DagContext,
    DagParams,
    Dags,
    InstrumentKeys,
    OpArgs,
    XComKeys,
)
from common.settings import (
    get_instrument_settings,
)
from common.utils import (
    get_airflow_variable,
    get_xcom,
    put_xcom,
    trigger_dag_run,
)
from file_handling import copy_file, get_file_hash, get_file_size
from raw_file_wrapper_factory import (
    CopyPathProvider,
    RawFileMonitorWrapper,
    RawFileWrapperFactory,
)

from shared.db.interface import get_raw_file_by_id, update_raw_file
from shared.db.models import (
    BackupStatus,
    RawFile,
    RawFileStatus,
    get_created_at_year_month,
)
from shared.keys import (
    DDA_FLAG_IN_RAW_FILE_NAME,
    FORBIDDEN_CHARACTERS_REGEXP,
)
from shared.yamlsettings import YamlKeys, get_path


def compute_checksum(ti: TaskInstance, **kwargs) -> bool:
    """Compute checksums for files in a raw file and store them in DB and XCom."""
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]

    raw_file = get_raw_file_by_id(raw_file_id)

    # TODO: this could be moved to an upfront task
    acquisition_monitor_errors = get_xcom(ti, XComKeys.ACQUISITION_MONITOR_ERRORS, [])
    if any(
        AcquisitionMonitorErrors.FILE_GOT_RENAMED in error
        for error in acquisition_monitor_errors
    ):
        logging.warning(
            f"Skipping copy for raw file {raw_file_id}: {acquisition_monitor_errors}"
        )

        update_raw_file(
            raw_file_id,
            new_status=RawFileStatus.ACQUISITION_FAILED,
            status_details=AcquisitionMonitorErrors.FILE_GOT_RENAMED,
            backup_status=BackupStatus.SKIPPED,
        )
        return False  # skip downstream tasks

    update_raw_file(raw_file_id, new_status=RawFileStatus.CHECKSUMMING)

    copy_wrapper = RawFileWrapperFactory.create_write_wrapper(
        raw_file=raw_file,
        path_provider=CopyPathProvider,
    )

    files_size_and_hashsum: dict[Path, tuple[float, str]] = {}
    files_dst_paths: dict[Path, Path] = {}
    file_info: dict[str, tuple[float, str]] = {}
    total_file_size = 0
    for src_path, dst_path in copy_wrapper.get_files_to_copy().items():
        file_size = get_file_size(src_path)
        total_file_size += file_size
        size_and_hashsum = (file_size, get_file_hash(src_path))

        files_dst_paths[src_path] = dst_path
        files_size_and_hashsum[src_path] = size_and_hashsum

        # file_info needs dst_path-related keys to correctly account for collisions
        file_info[str(dst_path.relative_to(copy_wrapper.target_folder_path))] = (
            size_and_hashsum
        )

    # to make this unusual situation transparent in UI:
    if not files_size_and_hashsum:
        raise AirflowFailException("No files were found!")

    if existing_file_info := raw_file.file_info:
        logging.warning(
            f"Raw file {raw_file_id} already has file_info, checking for equality."
        )

        if errors := _compare_file_info(existing_file_info, file_info):
            logging.warning(
                "File info mismatch detected:\n"
                f"{', '.join(errors)}\n"
                f"{existing_file_info=}\n"
                f"{file_info=}\n"
            )

            if (
                get_airflow_variable(AirflowVars.BACKUP_OVERWRITE_FILE_ID, "")
                == raw_file.id
            ):
                logging.warning(
                    f"Will overwrite existing file_info as requested by Airflow variable {AirflowVars.BACKUP_OVERWRITE_FILE_ID}."
                )
            else:
                logging.warning(
                    "This might be due to a previous checksumming operation being interrupted. \n"
                    "To resolve this issue: \n"
                    f"Set the Airflow Variable {AirflowVars.BACKUP_OVERWRITE_FILE_ID} to the ID of the raw file to force overwrite."
                )

                raise AirflowFailException(f"File info mismatch for {raw_file_id}")

    update_raw_file(
        raw_file_id,
        new_status=RawFileStatus.CHECKSUMMING_DONE,
        size=total_file_size,
        file_info=file_info,
    )

    put_xcom(
        ti,
        XComKeys.FILES_SIZE_AND_HASHSUM,
        {str(k): v for k, v in files_size_and_hashsum.items()},
    )
    put_xcom(
        ti,
        XComKeys.FILES_DST_PATHS,
        {str(k): str(v) for k, v in files_dst_paths.items()},
    )

    return True  # continue with downstream tasks


def _compare_file_info(
    existing_file_info: dict[str, tuple[float, str]],
    file_info: dict[str, tuple[float, str]],
) -> list[str]:
    """Compare existing file info with new file info and return a list of errors."""
    errors = []
    for file_name, size_and_hash in existing_file_info.items():
        if list(file_info.get(file_name, [])) != list(
            size_and_hash
        ):  # the existing file_info gets stored as a list
            errors.append(
                f"File info mismatch for {file_name}: existing {size_and_hash}, new {file_info.get(file_name)}"
            )
    if len(file_info) != len(existing_file_info):
        errors.append(
            f"File info length mismatch: existing {len(existing_file_info)}, new {len(file_info)}"
        )
    return errors


def copy_raw_file(ti: TaskInstance, **kwargs) -> None:
    """Copy all data associated with a raw file to the target location."""
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]

    files_dst_paths = {
        Path(k): Path(v) for k, v in get_xcom(ti, XComKeys.FILES_DST_PATHS).items()
    }
    files_size_and_hashsum = {
        Path(k): v for k, v in get_xcom(ti, XComKeys.FILES_SIZE_AND_HASHSUM).items()
    }

    raw_file = get_raw_file_by_id(raw_file_id)
    backup_base_path = get_backup_base_path(instrument_id, raw_file)

    update_raw_file(
        raw_file_id,
        new_status=RawFileStatus.COPYING,
        backup_base_path=str(backup_base_path),
        backup_status=BackupStatus.IN_PROGRESS,
    )

    if overwrite := (
        get_airflow_variable(AirflowVars.BACKUP_OVERWRITE_FILE_ID, "") == raw_file.id
    ):
        logging.warning(
            f"Will overwrite files as requested by Airflow variable {AirflowVars.BACKUP_OVERWRITE_FILE_ID}."
        )

    copied_files: dict[Path, tuple[float, str]] = {}
    for src_path, dst_path in files_dst_paths.items():
        src_size, src_hash = files_size_and_hashsum[src_path]
        dst_size, dst_hash = copy_file(
            src_path, dst_path, src_hash, overwrite=overwrite
        )
        copied_files[src_path] = (dst_size, dst_hash)

    try:
        _verify_copied_files(copied_files, files_dst_paths, files_size_and_hashsum)
    except ValueError as e:
        update_raw_file(
            raw_file_id,
            backup_status=BackupStatus.FAILED,
        )
        raise AirflowFailException(e) from e

    update_raw_file(
        raw_file_id,
        new_status=RawFileStatus.COPYING_DONE,
        backup_status=BackupStatus.DONE,
    )


def get_backup_base_path(instrument_id: str, raw_file: RawFile) -> Path:
    """Get the backup base path for the given instrument and raw file, e.g. /fs/pool/backup/test2/2025_07 ."""
    return (
        get_path(YamlKeys.Locations.BACKUP)
        / instrument_id
        / get_created_at_year_month(raw_file)
    )


def _verify_copied_files(
    copied_files: dict[Path, tuple[float, str]],
    files_dst_paths: dict[Path, Path],
    files_size_and_hashsum: dict[Path, tuple[float, str]],
) -> None:
    """Verify that the copied files match the original files in size and hash."""
    errors = []
    for src_path, (dst_size, dst_hash) in copied_files.items():
        src_size, src_hash = files_size_and_hashsum.get(src_path, (None, None))
        dst_path = files_dst_paths.get(src_path)
        if dst_size != src_size or dst_hash != src_hash:
            errors.append(
                f"Mismatch after copy: {src_path} {dst_path} {src_size} {dst_size} {src_hash} {dst_hash}"
            )
    if len(copied_files) != len(files_size_and_hashsum):
        errors.append(
            f"Length mismatch: {len(copied_files)=} != {len(files_size_and_hashsum)=}"
        )
    if errors:
        raise ValueError(f"File copy failed with errors: {','.join(errors)}")


def start_file_mover(ti: TaskInstance, **kwargs) -> None:
    """Trigger the file_mover DAG for a specific raw file."""
    del ti  # unused
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]

    time_delay_minutes = get_instrument_settings(
        instrument_id, InstrumentKeys.FILE_MOVE_DELAY_M
    )

    if time_delay_minutes < 0:
        logging.info(f"Skipping file mover for {raw_file_id=}: {time_delay_minutes=}")
        return

    trigger_dag_run(
        f"{Dags.FILE_MOVER}{DAG_DELIMITER}{instrument_id}",
        {
            DagParams.RAW_FILE_ID: raw_file_id,
        },
        time_delay_minutes=time_delay_minutes,
    )


def _count_special_characters(raw_file_id: str) -> int:
    """Check if the raw file name contains special characters."""
    pattern = re.compile(FORBIDDEN_CHARACTERS_REGEXP)
    return len(pattern.findall(raw_file_id))


def decide_processing(ti: TaskInstance, **kwargs) -> bool:
    """Decide whether to start the acquisition_processor DAG.

    Skip the downstream tasks if the raw file is not suitable for processing:
        - if the acquisition monitor has reported errors
        - if the raw file name contains the DDA flag
        - if the raw file name contains special characters
    """
    raw_file_id = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_ID]
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]
    acquisition_monitor_errors = get_xcom(ti, XComKeys.ACQUISITION_MONITOR_ERRORS, [])
    raw_file = get_raw_file_by_id(raw_file_id)

    if any(
        AcquisitionMonitorErrors.MAIN_FILE_MISSING in err
        for err in acquisition_monitor_errors
    ):
        new_status = RawFileStatus.ACQUISITION_FAILED
        status_details = ";".join(acquisition_monitor_errors)
    elif get_instrument_settings(instrument_id, InstrumentKeys.SKIP_QUANTING):
        new_status = RawFileStatus.DONE_NOT_QUANTED
        status_details = "Quanting disabled for this instrument."
    elif DDA_FLAG_IN_RAW_FILE_NAME in raw_file_id.lower():
        new_status = RawFileStatus.DONE_NOT_QUANTED
        status_details = "Filename contains 'dda'."
    elif _count_special_characters(raw_file_id):
        new_status = RawFileStatus.DONE_NOT_QUANTED
        status_details = "Filename contains special characters."
    elif raw_file.size == 0:
        new_status = RawFileStatus.ACQUISITION_FAILED
        status_details = "File size is zero."
    elif RawFileMonitorWrapper.is_corrupted_file_name(raw_file.original_name):
        new_status = RawFileStatus.ACQUISITION_FAILED
        status_details = "File name indicates failed acquisition."
    else:
        return True  # continue with downstream tasks

    logging.info(f"Skipping downstream tasks: {status_details=}")

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

    dag_id_to_trigger = f"{Dags.ACQUISITION_PROCESSOR}{DAG_DELIMITER}{instrument_id}"

    update_raw_file(raw_file_id, new_status=RawFileStatus.QUEUED_FOR_QUANTING)

    trigger_dag_run(
        dag_id_to_trigger,
        {
            DagParams.RAW_FILE_ID: raw_file_id,
        },
    )
