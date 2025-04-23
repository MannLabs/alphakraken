"""Constants."""

# prefix for the queues the DAGs are assigned to (cf. docker-compose.yaml)
AIRFLOW_QUEUE_PREFIX = "kraken_queue_"

CLUSTER_SSH_CONNECTION_ID = "cluster_ssh_connection"
CLUSTER_SSH_CONNECTION_TIMEOUT = 60
CLUSTER_SSH_COMMAND_TIMEOUT = 60

# these are relative to the SLURM_BASE_DIR environment variable
CLUSTER_JOB_SCRIPT_NAME = "submit_job.sh"
CLUSTER_WORKING_DIR_NAME = "jobs"


OUTPUT_FOLDER_PREFIX = "out_"

# separator between the timestamp and the raw file id in case of collisions
COLLISION_FLAG_SEP = "-"


BYTES_TO_GB = 1 / 1024**3
BYTES_TO_MB = 1 / 1024**2

# relevant for Bruker only
DEFAULT_RAW_FILE_SIZE_IF_MAIN_FILE_MISSING = -1

DEFAULT_MIN_FREE_SPACE_GB = -1  # will skip file removal

# mapping AlphaDIA errors to human-readable short names
ERROR_CODE_TO_STRING = {
    "_CANNOT_FIND_ITEM": "Cannot find item [Idx] within the current storage",
    "_FAILED_TO_DETERMINE_DIA_CYCLE": "Failed to determine start of DIA cycle",
    "_RUN_HEADER_EX": "RunHeaderEx",
    "_ARRAY_ERROR": "array must not contain infs or NaNs",
    "_FRAGMENT_MZ_TOLERANCE_MUST_BE_LESS_THAN": "fragment_mz_tolerance must be less than",
    "_PRECURSOR_MZ_TOLERANCE_MUST_BE_LESS_THAN": "precursor_mz_tolerance must be less than",
    "_NEED_AT_LEAST_ONE_ARRAY": "need at least one array to concatenate",
    "_NO_OBJECTS_TO_CONCATENATE": "No objects to concatenate",
    "_DATA_IN_LOW_DIM_SUBSPACE": "The data appears to lie in a lower-dimensional subspace of the space in which it is expressed",
    "_NO_CONSISTENT_SUBCYCLE_LENGTH": "No consistent subcycle length",
    "_TRAIN_SET_WILL_BE_EMPTY": "the resulting train set will be empty",
    "_CYCLE_NOT_CONSISTENT": "but does not consistent",
    "_NO_PSM_FILES": "No psm files accumulated",  # will become a known error in alphadia >1.7.2
    "_COMBINED_PSM_FILE_EMPTY": "combined psm file is empty, can't continue",
    "_NOT_DIA_DATA": "'TimsTOFTranspose' object has no attribute '_cycle'",  # will become a known error in alphadia >1.7.2
    "_KEY_MISSING_RT_CALIBRATED": "ERROR: 'rt_calibrated'",  # deliberately include "ERROR" here to be more specific
    "_KEY_MISSING_MZ_CALIBRATED": "ERROR: 'mz_calibrated'",  # deliberately include "ERROR" here to be more specific
    "_KEY_MISSING_MOBILITY_CALIBRATED": "ERROR: 'mobility_calibrated'",  # deliberately include "ERROR" here to be more specific
    "_FIRST_ARRAY_ELEMENT_EMPTY": "first array argument cannot be empty",
    "_ARGMAX_OF_EMPTY_SEQUENCE": "attempt to get argmax of an empty sequence",
    "_INTERNAL_ERROR_CANDIDATE_CONFIG": "'CandidateConfig' object has no attribute 'reporter'",
    "_SINGULAR_MATRIX": "Matrix is singular to machine precision",
    # the following are deliberately not included as they require a manual check:
    # "_FILE_IS_NOT_A_DATABASE": "file is not a database", # corrupt file? -> check if something went wrong with copying
    # "_COULD_NOT_OPEN_FILE": "could not be opened, is the file accessible", # hints at nonexisting file -> manual intervention?
    # "_OBJECT_REFERENCE_NOT_SET": "Object reference not set to an instance of an object", # corrupt file? -> check if something went wrong with copying
    # deliberately not including "DivisionByZero" here as it is too generic
}


class InternalPaths:
    """Paths to directories within the Docker containers."""

    MOUNTS_PATH = "/opt/airflow/mounts/"
    ENVS_PATH = "/opt/airflow/envs/"

    INSTRUMENTS = "instruments"
    BACKUP = "backup"
    OUTPUT = "output"


class Pools:
    """Pool names.

    cf. https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/pools.html
    """

    # pool to limit file copying across all instruments
    FILE_COPY_POOL = "file_copy_pool"  # suggested default: 3

    # pool to limit the number of concurrent jobs on the cluster
    CLUSTER_SLOTS_POOL = "cluster_slots_pool"  #  suggested default: 30


class AlphaDiaConstants:
    """Constants for accessing AlphaDia output."""

    LOG_FILE_NAME = "log.txt"
    EVENTS_FILE_NAME = "events.jsonl"
    PROGRESS_FOLDER_NAME = "quant"
