"""Keys for accessing Dags, Tasks, etc.."""

DAG_DELIMITER: str = "."


class Dags:
    """Dag Names."""

    ACQUISITON_WATCHER: str = "acquisition_watcher"
    ACQUISITON_HANDLER: str = "acquisition_handler"


class Tasks:
    """Task Names."""

    WAIT_FOR_FILE_CREATION: str = "wait_for_file_creation"
    CHECK_DB: str = "check_db"
    CHECK_PROJECT_ID: str = "check_project_id"
    START_ACQUISITION_HANDLER: str = "start_acquisition_handler"

    ADD_TO_DB: str = "add_to_db"
    PREPARE_QUANTING: str = "prepare_quanting"
    RUN_QUANTING: str = "run_quanting"
    MONITOR_QUANTING: str = "monitor_quanting"
    GET_JOB_INFO: str = "get_job_info"
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
    RAW_FILE_NAME: str = "raw_file_name"


class XComKeys:
    """Keys for accessing XCom."""

    RAW_FILE_NAME: str = "raw_file_name"
    RAW_FILE_NAMES: str = "raw_file_names"

    JOB_ID: str = "job_id"

    METRICS: str = "metrics"
    TIME_ELAPSED: str = "time_elapsed"


class InstrumentKeys:
    """Keys for accessing instrument data."""

    RAW_DATA_PATH = "raw_data_path"


class Variables:
    """Keys for accessing Airflow Variables."""

    DEBUG_NO_CLUSTER_SSH = "debug_no_cluster_ssh"
