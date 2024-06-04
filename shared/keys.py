"""Keys for accessing Dags, Tasks, etc.."""

DAG_DELIMITER: str = "."


class Dags:
    """Dag Names."""

    ACQUISITON_WATCHER: str = "acquisition_watcher"
    ACQUISITON_HANDLER: str = "acquisition_handler"


class Tasks:
    """Task Names."""

    START_ACQUISITION_HANDLER: str = "start_acquisition_handler"
    WAIT_FOR_FINISHED_ACQUISITION: str = "wait_for_finished_acquisition"

    ADD_TO_DB: str = "add_to_db"
    PREPARE_QUANTING: str = "prepare_quanting"
    RUN_QUANTING: str = "run_quanting"
    MONITOR_QUANTING: str = "monitor_quanting"
    COMPUTE_METRICS: str = "compute_metrics"
    UPLOAD_METRICS: str = "upload_metrics"


class OpArgs:
    """Keys for passing arguments to operators."""

    INSTRUMENT_ID: str = "instrument_id"


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


class InstrumentKeys:
    """Keys for accessing instrument data."""

    RAW_DATA_PATH = "raw_data_path"
