"""Keys for accessing Dags, Tasks, etc.."""

DAG_DELIMITER: str = "."


QUANTING_TIME_ELAPSED_METRIC: str = "quanting_time_elapsed"


class Dags:
    """Dag Names."""

    ACQUISITION_WATCHER: str = "instrument_watcher"
    ACQUISITION_HANDLER: str = "acquisition_handler"
    ACQUISITION_PROCESSOR: str = "acquisition_processor"
    FILE_MOVER: str = "file_mover"
    FILE_REMOVER: str = "file_remover"
    S3_UPLOADER: str = "s3_uploader"


class Tasks:
    """Task Names."""

    WAIT_FOR_RAW_FILE_CREATION: str = "wait_for_raw_file_creation"
    GET_UNKNOWN_RAW_FILES: str = "get_unknown_raw_files"
    DECIDE_HANDLING: str = "decide_handling"
    START_ACQUISITION_HANDLER: str = "start_acquisition_handler"

    MONITOR_ACQUISITION: str = "monitor_acquisition"
    COMPUTE_CHECKSUM: str = "compute_checksum"
    COPY_RAW_FILE: str = "copy_raw_file"
    UPLOAD_TO_S3: str = "upload_to_s3"
    START_S3_UPLOADER: str = "start_s3_uploader"
    START_FILE_MOVER: str = "start_file_mover"
    DECIDE_PROCESSING: str = "decide_processing"
    START_ACQUISITION_PROCESSOR: str = "start_acquisition_processor"

    PREPARE_QUANTING: str = "prepare_quanting"
    RUN_MSQC: str = "run_msqc"
    MONITOR_MSQC: str = "monitor_msqc"
    COMPUTE_MSQC_METRICS: str = "compute_msqc_metrics"
    UPLOAD_MSQC_METRICS: str = "upload_msqc_metrics"

    RUN_QUANTING: str = "run_quanting"
    WAIT_FOR_JOB_START: str = "wait_for_job_start"
    MONITOR_QUANTING: str = "monitor_quanting"
    CHECK_QUANTING_RESULT: str = "check_quanting_result"
    COMPUTE_METRICS: str = "compute_metrics"
    UPLOAD_METRICS: str = "upload_metrics"

    MOVE_RAW_FILES: str = "move_raw_file"
    GET_FILES_TO_MOVE: str = "get_files_to_move"

    REMOVE_RAW_FILES: str = "remove_raw_files"
    GET_RAW_FILES_TO_REMOVE: str = "get_raw_files_to_remove"


class OpArgs:
    """Keys for passing arguments to operators."""

    INSTRUMENT_ID: str = "instrument_id"


class DagContext:
    """Keys for accessing context in DAGs."""

    PARAMS: str = "params"


class DagParams:
    """Keys for accessing parameters in DAG context 'params'."""

    # "params" level
    RAW_FILE_ID: str = "raw_file_id"

    # s3 uploader
    INTERNAL_TARGET_FOLDER_PATH: str = "internal_target_folder_path"


class XComKeys:
    """Keys for accessing XCom."""

    RAW_FILE_ID: str = "raw_file_id"
    RAW_FILE_NAMES_TO_PROCESS: str = "raw_file_names_to_process"
    RAW_FILE_NAMES_WITH_DECISIONS: str = "raw_file_names_with_decisions"

    ACQUISITION_MONITOR_ERRORS: str = "acquisition_monitor_errors"

    FILES_SIZE_AND_HASHSUM: str = "files_size_and_hashsum"
    FILES_DST_PATHS: str = "files_dst_paths"
    TARGET_FOLDER_PATH: str = "target_folder_path"

    QUANTING_ENV: str = "quanting_env"

    JOB_ID: str = "job_id"
    MSQC_JOB_ID: str = "msqc_job_id"

    METRICS: str = "metrics"
    METRICS_TYPE: str = "metrics_type"
    QUANTING_TIME_ELAPSED: str = "quanting_time_elapsed"

    FILES_TO_MOVE = "files_to_move"
    MAIN_FILE_TO_MOVE = "main_file_to_move"
    FILES_TO_REMOVE = "files_to_remove"
    INSTRUMENTS_WITH_ERRORS = "instrument_with_errors"


class InstrumentKeys:
    """Keys for accessing instrument data.

    If defaults need to be set, use the INSTRUMENT_SETTINGS_DEFAULTS dictionary in settings.py.
    """

    TYPE: str = "type"
    # instrument-specific settings
    SKIP_QUANTING: str = "skip_quanting"
    MIN_FREE_SPACE_GB: str = "min_free_space_gb"
    FILE_MOVE_DELAY_M: str = "file_move_delay_m"


class AirflowVars:
    """Keys for accessing Airflow Variables (set in the Airflow UI). Cf. also Readme."""

    # set to a specific file name to allow overwriting it on the checksum computation
    CHECKSUM_OVERWRITE_FILE_ID = "checksum_overwrite_file_id"

    # set to a specific file name to allow overwriting it on the pool backup
    BACKUP_OVERWRITE_FILE_ID = "backup_overwrite_file_id"

    # whether to consider acquisition "done" if current file is "old" (> 5h) compared to youngest file
    CONSIDER_OLD_FILES_ACQUIRED = "consider_old_files_acquired"

    # for special modes 'overwrite' or 'associated' (see docs)
    OUTPUT_EXISTS_MODE: str = "output_exists_mode"

    # The minimum file age in days for files to be removed by the file_remover.
    MIN_FILE_AGE_TO_REMOVE_IN_DAYS: str = "min_file_age_to_remove_in_days"

    # Minimum free space that should be left after file removal.
    MIN_FREE_SPACE_GB: str = "min_free_space_gb"

    # some flags that can be used for debugging and/or to simplify the local setup
    DEBUG_NO_CLUSTER_SSH = "debug_no_cluster_ssh"

    # maximum file age in hours to be picked up by the quanting handler. Older files get status 'ignored'
    # and will not be backed up or quanted.
    DEBUG_MAX_FILE_AGE_IN_HOURS: str = "debug_max_file_age_in_hours"


class QuantingEnv:
    """Keys for setting quanting environment variables for the slurm submit script."""

    RAW_FILE_PATH = "RAW_FILE_PATH"
    OUTPUT_PATH = "OUTPUT_PATH"
    RELATIVE_OUTPUT_PATH = "RELATIVE_OUTPUT_PATH"
    SETTINGS_PATH = "SETTINGS_PATH"

    SPECLIB_FILE_NAME = "SPECLIB_FILE_NAME"
    FASTA_FILE_NAME = "FASTA_FILE_NAME"
    CONFIG_FILE_NAME = "CONFIG_FILE_NAME"
    SOFTWARE = "SOFTWARE"
    SOFTWARE_TYPE = "SOFTWARE_TYPE"
    CUSTOM_COMMAND = "CUSTOM_COMMAND"

    PROJECT_ID_OR_FALLBACK: str = "PROJECT_ID_OR_FALLBACK"
    RAW_FILE_ID: str = "RAW_FILE_ID"
    SETTINGS_NAME: str = "SETTINGS_NAME"
    SETTINGS_VERSION: str = "SETTINGS_VERSION"

    NUM_THREADS: str = "NUM_THREADS"

    # those will not be exported as environment variables due to the leading underscore
    SLURM_CPUS_PER_TASK: str = "_SLURM_CPUS_PER_TASK"
    SLURM_MEM: str = "_SLURM_MEM"
    SLURM_TIME: str = "_SLURM_TIME"


class JobStates:
    """States of a slurm job as returned by the sacct command, cf slurm_commands.py:get_job_state_cmd()."""

    PENDING: str = "PENDING"
    RUNNING: str = "RUNNING"
    COMPLETED: str = "COMPLETED"
    FAILED: str = "FAILED"
    TIMEOUT: str = "TIMEOUT"
    OUT_OF_MEMORY: str = "OUT_OF_ME"  # it is displayed "OUT_OF_ME+", so only startwith() comparisons here


class CustomAlphaDiaStates:
    """Alphakraken-custom states to handle errors during retrieval of unknown alphaDIA errors."""

    NO_LOG_FILE: str = "__NO_LOG_FILE"
    UNKNOWN_ERROR: str = "__UNKNOWN_ERROR"
    COULD_NOT_DETERMINE_ERROR: str = "__COULD_NOT_DETERMINE_ERROR"


class AcquisitionMonitorErrors:
    """Errors that can occur during acquisition monitoring."""

    MAIN_FILE_MISSING: str = "Main file was not created"
    FILE_GOT_RENAMED: str = "File got renamed"
