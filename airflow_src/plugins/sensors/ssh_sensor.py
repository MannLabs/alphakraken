"""SSH sensor operator."""

import logging
from abc import ABC, abstractmethod

from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.sensors.base import BaseSensorOperator
from cluster_scripts.slurm_commands import get_job_state_cmd
from common.keys import AirflowVars, XComKeys
from common.utils import get_airflow_variable, get_xcom


class SSHSensorOperator(BaseSensorOperator, ABC):
    """Wait for a ssh command to return a certain output."""

    @property
    @abstractmethod
    def command_template(self) -> str:
        """Template for the command to execute.

        Must be a bash script that is executable on the cluster.
        Its only output to stdout must be the status of the queried job.
        The following placeholders are available and will be substituted before running:
            - REPLACE_JID: the job id to query
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

        command = self.command_template.replace("REPLACE_JID", self._job_id)

        ssh_return = self.ssh_execute(command, self._ssh_hook)
        logging.info(f"ssh command returned: '{ssh_return}'")

        if ssh_return in self.running_states:
            return False

        return True

    @staticmethod
    def ssh_execute(
        command: str,
        ssh_hook: SSHHook,
    ) -> str:
        """Execute the given `command` via the `ssh_hook`."""
        # this is a hack to prevent jobs to be run on the cluster, useful for debugging and initial setup.
        # TODO: needs to be improved, maybe by setting up a container with a fake ssh server
        if get_airflow_variable(AirflowVars.DEBUG_NO_CLUSTER_SSH, "False") == "True":
            return SSHSensorOperator._get_fake_ssh_response(command)

        exit_status, agg_stdout, agg_stderr = ssh_hook.exec_ssh_client_command(
            ssh_hook.get_conn(),
            command,
            timeout=60,
            get_pty=False,
            environment={},
        )
        logging.info(f"Got {exit_status=} {agg_stdout=} {agg_stderr=}")
        return agg_stdout.decode("utf-8").strip()

    @staticmethod
    def _get_fake_ssh_response(command: str) -> str:
        """Fake a ssh response for the given `command`."""
        logging.warning(
            f"Variable {AirflowVars.DEBUG_NO_CLUSTER_SSH} set: Not running SSH command on cluster:\n{command}"
        )
        # very heuristic way to return a fake response
        if "sbatch" in command:  # run job
            return "something\nsomething\n123"
        if "quanting_time_elapsed" in command:  # get job info
            return "00:00:01"
        return "COMPLETED"  # monitor job


class QuantingSSHSensor(SSHSensorOperator):
    """Monitor the status of a quanting job on the SLURM cluster."""

    @property
    def command_template(self) -> str:
        """See docu of superclass."""
        return get_job_state_cmd()

    @property
    def running_states(self) -> list[str]:
        """States that are considered 'running'."""
        return ["PENDING", "RUNNING"]
