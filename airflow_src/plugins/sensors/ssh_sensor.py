"""SSH sensor operator."""

import logging
from abc import ABC, abstractmethod
from time import sleep

from airflow.exceptions import AirflowFailException
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.sensors.base import BaseSensorOperator
from cluster_scripts.slurm_commands import get_job_state_cmd
from common.keys import AirflowVars, JobStates, XComKeys
from common.utils import (
    get_airflow_variable,
    get_cluster_ssh_hook,
    get_xcom,
    truncate_string,
)


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
    def states(self) -> list[str]:
        """Outputs of the command in `command_template` that are considered 'running'."""

    def __init__(self, *args, **kwargs):
        """Initialize the operator."""
        super().__init__(*args, **kwargs)
        self._ssh_hook: SSHHook = get_cluster_ssh_hook()
        self._job_id: str | None = None

    def pre_execute(self, context: dict[str, any]) -> None:
        """_job_id the job id from XCom."""
        self._job_id = get_xcom(context["ti"], XComKeys.JOB_ID)

    def poke(self, context: dict[str, any]) -> bool:
        """Check the output of the ssh command."""
        del context  # unused

        ssh_return = self.ssh_execute(self.command, self._ssh_hook)
        logging.info(f"ssh command returned: '{ssh_return}'")

        if ssh_return in self.states:
            return False

        return True

    # show file size earlier

    @staticmethod
    def ssh_execute(
        command: str,
        ssh_hook: SSHHook | None = None,
        *,
        max_tries: int = 10,
    ) -> str:
        """Execute the given `command` via the `ssh_hook`.

        Sometimes the SSH command returns a nonzero exit status '254' or empty byte string,
        in this case it is retried until it is 200 and nonempty until `max_tries` is reached.
        """
        # This is a hack to prevent jobs to be run on the cluster, useful for debugging and initial setup.
        # To get rid of this, e.g. set up a container with a fake ssh server
        if get_airflow_variable(AirflowVars.DEBUG_NO_CLUSTER_SSH, "False") == "True":
            return SSHSensorOperator._get_fake_ssh_response(command)

        if ssh_hook is None:
            ssh_hook = get_cluster_ssh_hook()

        str_stdout = ""
        exit_status = None
        agg_stdout = None
        call_count = 0
        while exit_status != 0 or agg_stdout in [b"", b"\n"]:
            sleep(5 * call_count)  # no sleep in the first iteration
            call_count += 1
            if call_count >= max_tries:
                raise AirflowFailException(f"Too many calls to ssh_execute: {command=}")

            exit_status, agg_stdout, agg_stderr = ssh_hook.exec_ssh_client_command(
                ssh_hook.get_conn(),
                command,
                timeout=60,
                get_pty=False,
                environment={},
            )
            str_stdout = SSHSensorOperator._byte_to_string(agg_stdout)
            str_stdout_trunc = truncate_string(str_stdout)
            logging.info(
                f"ssh command call {call_count+1} returned: {exit_status=} {str_stdout_trunc=} {agg_stderr=}"
            )

        return str_stdout

    @staticmethod
    def _byte_to_string(input_: bytes) -> str:
        """Convert the given `input_` to a string."""
        return input_.decode("utf-8").strip()

    @staticmethod
    def _get_fake_ssh_response(command: str) -> str:
        """Fake an ssh response for the given `command`.

        Only for testing and debugging.
        """
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


class WaitForJobStartSSHSensor(SSHSensorOperator):
    """Wait until a SLURM job leaves status 'PENDING'."""

    @property
    def command(self) -> str:
        """See docu of superclass."""
        return get_job_state_cmd(self._job_id)

    @property
    def states(self) -> list[str]:
        """List of states that keep the sensor waiting."""
        return [JobStates.PENDING]


class QuantingSSHSensor(SSHSensorOperator):
    """Wait until a SLURM job leaves status 'RUNNING'."""

    @property
    def command(self) -> str:
        """See docu of superclass."""
        return get_job_state_cmd(self._job_id)

    @property
    def states(self) -> list[str]:
        """List of states that keep the sensor waiting."""
        return [JobStates.RUNNING]
