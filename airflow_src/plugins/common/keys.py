"""Keys for accessing Dags, Tasks, etc.."""

DAG_DELIMITER: str = "."


class Dags:
    """Dag Names."""

    ACQUISITON_WATCHER: str = "instrument_watcher"
    ACQUISITION_HANDLER: str = "acquisition_handler"
    ACQUISITON_HANDLER: str = "acquisition_processor"


class Tasks:
    """Task Names."""

    WAIT_FOR_FILE_CREATION: str = "wait_for_file_creation"
    GET_UNKNOWN_RAW_FILES: str = "get_unknown_raw_files"
    DECIDE_HANDLING: str = "decide_handling"
    START_ACQUISITION_HANDLER: str = "start_acquisition_handler"

    MONITOR_ACQUISITION: str = "monitor_acquisition"
    COPY_RAW_DATA: str = "copy_raw_data"
    START_ACQUISITION_PROCESSOR: str = "start_acquisition_processor"

    PREPARE_QUANTING: str = "prepare_quanting"
    RUN_QUANTING: str = "run_quanting"
    MONITOR_QUANTING: str = "monitor_quanting"
    CHECK_QUANTING_RESULT: str = "check_quanting_result"
    COMPUTE_METRICS: str = "compute_metrics"
    UPLOAD_METRICS: str = "upload_metrics"


class OpArgs:
    """Keys for passing arguments to operators."""

    INSTRUMENT_ID: str = "instrument_id"
    SSH_HOOK: str = "ssh_hook"
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

    QUANTING_ENV: str = "quanting_env"

    JOB_ID: str = "job_id"

    METRICS: str = "metrics"
    QUANTING_TIME_ELAPSED: str = "quanting_time_elapsed"


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

    # maximum file age in hours to be picked up by the quanting handler. Older files get status 'ignored'.
    MAX_FILE_AGE_IN_HOURS: str = "max_file_age_in_hours"

    # If set to True, quanting can be started even if the output folder already exists.
    ALLOW_OUTPUT_OVERWRITE: str = "allow_output_overwrite"

    # some flags that can be used for debugging and/or to simplify the local setup
    DEBUG_NO_CLUSTER_SSH = "debug_no_cluster_ssh"


class QuantingEnv:
    """Keys for setting quanting environment variables for the slurm submit script."""

    RAW_FILE_ID = "RAW_FILE_ID"
    INPUT_DATA_REL_PATH = "INPUT_DATA_REL_PATH"
    OUTPUT_FOLDER_REL_PATH = "OUTPUT_FOLDER_REL_PATH"
    SPECLIB_FILE_NAME = "SPECLIB_FILE_NAME"

    FASTA_FILE_NAME = "FASTA_FILE_NAME"
    CONFIG_FILE_NAME = "CONFIG_FILE_NAME"
    SOFTWARE = "SOFTWARE"
    PROJECT_ID_OR_FALLBACK = "PROJECT_ID_OR_FALLBACK"
    IO_POOL_FOLDER = "IO_POOL_FOLDER"


class JobStates:
    """States of a slurm job as returned by the sacct command, cf slurm_commands.py:get_job_state_cmd()."""

    PENDING: str = "PENDING"
    RUNNING: str = "RUNNING"
    COMPLETED: str = "COMPLETED"
    FAILED: str = "FAILED"
