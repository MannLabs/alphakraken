"""SSH sensor operator."""

import logging
from abc import ABC, abstractmethod
from time import sleep

from airflow.exceptions import AirflowFailException
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.sensors.base import BaseSensorOperator
from cluster_scripts.slurm_commands import get_job_state_cmd
from common.keys import AirflowVars, JobStates, XComKeys
from common.utils import get_airflow_variable, get_xcom


class SSHSensorOperator(BaseSensorOperator, ABC):
    """Wait for a ssh command to return a certain output."""

    @property
    @abstractmethod
    def command(self) -> str:
        """The command to execute.

        Must be a bash script that is executable on the cluster.
        Its only output to stdout must be the status of the queried job.
        """

    @property
    @abstractmethod
    def running_states(self) -> list[str]:
        """Outputs of the command in `command_template` that are considered 'running'."""

    def __init__(self, ssh_hook: SSHHook, *args, **kwargs):
        """Initialize the operator.

        :param ssh_hook: the ssh hook to use.
        """
        super().__init__(*args, **kwargs)
        self._ssh_hook: SSHHook = ssh_hook
        self._job_id: str | None = None

    def pre_execute(self, context: dict[str, any]) -> None:
        """_job_id the job id from XCom."""
        self._job_id = get_xcom(context["ti"], XComKeys.JOB_ID)

    def poke(self, context: dict[str, any]) -> bool:
        """Check the output of the ssh command."""
        logging.info(f"SSH command execute: {context}")

        ssh_return = self.ssh_execute(self.command, self._ssh_hook)
        logging.info(f"ssh command returned: '{ssh_return}'")

        if ssh_return in self.running_states:
            return False

        return True

    @staticmethod
    def ssh_execute(command: str, ssh_hook: SSHHook, max_tries: int = 10) -> str:
        """Execute the given `command` via the `ssh_hook`."""
        # this is a hack to prevent jobs to be run on the cluster, useful for debugging and initial setup.
        # To get rid of this, e.g. set up a container with a fake ssh server
        if get_airflow_variable(AirflowVars.DEBUG_NO_CLUSTER_SSH, "False") == "True":
            return SSHSensorOperator._get_fake_ssh_response(command)

        # Sometimes the SSH command returns an exit status '254' or empty byte string,
        # so retry some time until it is 200 and nonempty
        exit_status = -1
        agg_stdout = b""
        call_count = 0
        while exit_status != 0 or agg_stdout in [b"", b"\n"]:
            if exit_status != -1:
                sleep(5 * call_count)
            exit_status, agg_stdout, agg_stderr = ssh_hook.exec_ssh_client_command(
                ssh_hook.get_conn(),
                command,
                timeout=60,
                get_pty=False,
                environment={},
            )
            logging.info(
                f"ssh command call {call_count+1} returned: {exit_status=} {agg_stdout=} {agg_stderr=}"
            )

            call_count += 1
            if call_count >= max_tries:
                raise AirflowFailException(f"Too many calls to ssh_execute: {command=}")

        return agg_stdout.decode("utf-8").strip()

    @staticmethod
    def _get_fake_ssh_response(command: str) -> str:
        """Fake a ssh response for the given `command`."""
        logging.warning(
            f"Variable {AirflowVars.DEBUG_NO_CLUSTER_SSH} set: Not running SSH command on cluster:\n{command}"
        )
        # very heuristic way to decide which fake response to return
        if "sbatch" in command:  # run job
            response = "something\nsomething\n123"
        elif "TIME_ELAPSED" in command:  # get job info
            response = f"00:00:01\nsomething\n{JobStates.COMPLETED}"
        else:
            response = JobStates.COMPLETED  # monitor job

        logging.warning(f"Returning fake response: {response}")
        return response


class QuantingSSHSensor(SSHSensorOperator):
    """Monitor the status of a quanting job on the SLURM cluster."""

    @property
    def command(self) -> str:
        """See docu of superclass."""
        return get_job_state_cmd(self._job_id)

    @property
    def running_states(self) -> list[str]:
        """States that are considered 'running'."""
        return [JobStates.PENDING, JobStates.RUNNING]
