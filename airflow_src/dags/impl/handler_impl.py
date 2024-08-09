"""Business logic for the acquisition_handler."""

import logging

from airflow.models import TaskInstance
from common.keys import DagContext, DagParams, OpArgs, XComKeys
from common.settings import get_instrument_data_path
from common.utils import get_xcom, put_xcom
from sensors.ssh_sensor import SSHSensorOperator

from shared.db.engine import RawFile, RawFileStatus, add_new_raw_file_to_db, connect_db


def add_to_db(ti: TaskInstance, **kwargs) -> None:
    """Add the file to the database with initial status and basic information."""
    # example how to retrieve parameters from the context
    raw_file_name = kwargs[DagContext.PARAMS][DagParams.RAW_FILE_NAME]
    instrument_id = kwargs[OpArgs.INSTRUMENT_ID]

    logging.info(f"Got {raw_file_name=} on {instrument_id=}")

    # TODO: exception handling: retry vs noretry

    raw_file_path = get_instrument_data_path(instrument_id) / raw_file_name
    raw_file_size = raw_file_path.stat().st_size
    raw_file_creation_time = raw_file_path.stat().st_ctime
    logging.info(f"Got {raw_file_size / 1024**3} GB {raw_file_creation_time}")

    add_new_raw_file_to_db(
        raw_file_name,
        instrument_id=instrument_id,
        size=raw_file_size,
        creation_ts=raw_file_creation_time,
    )

    # push to XCOM
    put_xcom(ti, XComKeys.RAW_FILE_NAME, raw_file_name)


def prepare_quanting(ti: TaskInstance, **kwargs) -> None:
    """TODO."""
    del ti
    del kwargs
    # IMPLEMENT:
    # create the alphadia inputfile and store it on the shared volume


# TODO: put this somewhere else
# TODO: how to bring 'run.sh' to the cluster?
# Must be a bash script that is executable on the cluster.
# Its only output to stdout must be the job id of the submitted job.
run_quanting_cmd = """
cd ~/kraken &&
JID=$(sbatch run.sh)
echo ${JID##* }
"""


def run_quanting(ti: TaskInstance, **kwargs) -> None:
    """Run the quanting job on the cluster."""
    # IMPLEMENT:
    # wait for the cluster to be ready (20% idling) -> dedicated (sensor) task

    ssh_hook = kwargs[OpArgs.SSH_HOOK]

    raw_file_name = get_xcom(ti, XComKeys.RAW_FILE_NAME)
    export_cmd = f"export RAW_FILE_NAME={raw_file_name}\n"

    command = export_cmd + run_quanting_cmd
    logging.info(f"Running command: >>>>\n{command}\n<<<< end of command")

    # TODO: prevent cluster from overfeeding on stall
    # TODO: prevent re-starting the same job again (SBATCH unique key or smth?)
    job_id = SSHSensorOperator.ssh_execute(command, ssh_hook)

    # TODO: fail on empty job id

    put_xcom(ti, XComKeys.JOB_ID, job_id)


def compute_metrics(ti: TaskInstance, **kwargs) -> None:
    """TODO."""
    del ti
    del kwargs

    # IMPLEMENT:
    # compute metrics from the output files
    # store them locally (?)


def upload_metrics(ti: TaskInstance, **kwargs) -> None:
    """TODO."""
    del kwargs

    raw_file_name = get_xcom(ti, XComKeys.RAW_FILE_NAME)
    connect_db()

    raw_file = RawFile.objects.with_id(raw_file_name)
    # example: update raw file status

    logging.info(f"got {raw_file=}")
    raw_file.update(status=RawFileStatus.PROCESSED)

    # sanity check:
    for raw_file in RawFile.objects(name=raw_file_name):
        logging.info(f"{raw_file.name} {raw_file.status}")

    # IMPLEMENT:
    # put metrics to the database
