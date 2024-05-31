"""Keys for accessing Dags, Tasks, etc.."""


class Dags:
    """Dag Names."""

    ACQUISITON_WATCHER: str = "acquisition_watcher"
    ACQUISITON_HANDLER: str = "acquisition_handler"


class Tasks:
    """Task Names."""

    START_ACQUISITION_HANDLER: str = "start_acquisition_handler"
    WAIT_FOR_FINISHED_ACQUISITION: str = "wait_for_finished_acquisition"

    PREPARE_QUANTING: str = "prepare_quanting"
    RUN_QUANTING: str = "run_quanting"
    MONITOR_QUANTING: str = "monitor_quanting"
    COMPUTE_METRICS: str = "compute_metrics"
    UPLOAD_METRICS: str = "upload_metrics"
