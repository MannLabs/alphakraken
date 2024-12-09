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


class Tasks:
    """Task Names."""

    WAIT_FOR_RAW_FILE_CREATION: str = "wait_for_raw_file_creation"
    GET_UNKNOWN_RAW_FILES: str = "get_unknown_raw_files"
    DECIDE_HANDLING: str = "decide_handling"
    START_ACQUISITION_HANDLER: str = "start_acquisition_handler"

    MONITOR_ACQUISITION: str = "monitor_acquisition"
    COPY_RAW_FILE: str = "copy_raw_file"
    START_FILE_MOVER: str = "start_file_mover"
    DECIDE_PROCESSING: str = "decide_processing"
    START_ACQUISITION_PROCESSOR: str = "start_acquisition_processor"

    PREPARE_QUANTING: str = "prepare_quanting"
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
    COMMAND: str = "command"


class DagContext:
    """Keys for accessing context in DAGs."""

    PARAMS: str = "params"


class DagParams:
    """Keys for accessing parameters in DAG context 'params'."""

    # "params" level
    RAW_FILE_ID: str = "raw_file_id"


class XComKeys:
    """Keys for accessing XCom."""

    RAW_FILE_ID: str = "raw_file_id"
    RAW_FILE_NAMES_TO_PROCESS: str = "raw_file_names_to_process"
    RAW_FILE_NAMES_WITH_DECISIONS: str = "raw_file_names_with_decisions"

    ACQUISITION_MONITOR_ERRORS: str = "acquisition_monitor_errors"

    QUANTING_ENV: str = "quanting_env"

    JOB_ID: str = "job_id"

    METRICS: str = "metrics"
    QUANTING_TIME_ELAPSED: str = "quanting_time_elapsed"

    FILES_TO_MOVE = "files_to_move"
    MAIN_FILE_TO_MOVE = "main_file_to_move"
    FILES_TO_REMOVE = "files_to_remove"
    INSTRUMENTS_WITH_ERRORS = "instrument_with_errors"


class InstrumentKeys:
    """Keys for accessing instrument data."""

    TYPE: str = "type"


class InstrumentTypes:
    """Types of instruments."""

    THERMO: str = "thermo"
    BRUKER: str = "bruker"
    ZENO: str = "zeno"


class AirflowVars:
    """Keys for accessing Airflow Variables (set in the Airflow UI). Cf. also Readme."""

    # If set to True, quanting can be started even if the output folder already exists.
    ALLOW_OUTPUT_OVERWRITE: str = "allow_output_overwrite"

    # The minimum file age in days for files to be removed by the file_remover.
    MIN_FILE_AGE_TO_REMOVE_IN_DAYS: str = "min_file_age_to_remove_in_days"

    # Minimum free space that should be left after file removal.
    MIN_FREE_SPACE_GB: str = "min_free_space_gb"

    # set to a specific file name to allow overwriting it on the pool backup
    BACKUP_OVERWRITE_FILE_ID = "backup_overwrite_file_id"

    # some flags that can be used for debugging and/or to simplify the local setup
    DEBUG_NO_CLUSTER_SSH = "debug_no_cluster_ssh"

    # maximum file age in hours to be picked up by the quanting handler. Older files get status 'ignored'
    # and will not be backed up or quanted.
    DEBUG_MAX_FILE_AGE_IN_HOURS: str = "debug_max_file_age_in_hours"


class QuantingEnv:
    """Keys for setting quanting environment variables for the slurm submit script."""

    RAW_FILE_PATH = "RAW_FILE_PATH"
    OUTPUT_PATH = "OUTPUT_PATH"
    SETTINGS_PATH = "SETTINGS_PATH"

    SPECLIB_FILE_NAME = "SPECLIB_FILE_NAME"
    FASTA_FILE_NAME = "FASTA_FILE_NAME"
    CONFIG_FILE_NAME = "CONFIG_FILE_NAME"
    SOFTWARE = "SOFTWARE"

    PROJECT_ID_OR_FALLBACK: str = "PROJECT_ID_OR_FALLBACK"
    RAW_FILE_ID: str = "RAW_FILE_ID"
    SETTINGS_VERSION: str = "SETTINGS_VERSION"


class JobStates:
    """States of a slurm job as returned by the sacct command, cf slurm_commands.py:get_job_state_cmd()."""

    PENDING: str = "PENDING"
    RUNNING: str = "RUNNING"
    COMPLETED: str = "COMPLETED"
    FAILED: str = "FAILED"
    TIMEOUT: str = "TIMEOUT"


class CustomAlphaDiaStates:
    """Alphakraken-custom states to handle errors during retrieval of unknown alphaDIA errors."""

    NO_LOG_FILE: str = "__NO_LOG_FILE"
    UNKNOWN_ERROR: str = "__UNKNOWN_ERROR"
    COULD_NOT_DETERMINE_ERROR: str = "__COULD_NOT_DETERMINE_ERROR"


class AcquisitionMonitorErrors:
    """Errors that can occur during acquisition monitoring."""

    MAIN_FILE_MISSING: str = "Main file was not created: "
