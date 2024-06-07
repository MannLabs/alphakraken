"""SSH sensor operator."""

import logging
from abc import ABC, abstractmethod

from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.sensors.base import BaseSensorOperator
from common.keys import XComKeys
from common.utils import get_xcom


class SSHSensorOperator(BaseSensorOperator, ABC):
    """Wait for some ssh command to succeed."""

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
        """States that are considered 'running'."""

    def __init__(self, ssh_hook: SSHHook, *args, **kwargs):
        """Initialize the operator.

        :param ssh_hook: the ssh hook to use.
        """
        super().__init__(*args, **kwargs)
        self.ssh_hook = ssh_hook

    def poke(self, context: dict[str, any]) -> bool:
        """Check the output of the ssh command."""
        logging.info(f"SSH command execute: {context}")

        jid = get_xcom(context["ti"], XComKeys.JOB_ID)

        command = self.command_template.replace("REPLACE_JID", jid)

        ssh_return = self.ssh_execute(command, self.ssh_hook)

        logging.info("SSH command returned: '%s'", ssh_return)

        if ssh_return in self.running_states:
            return False

        return True

    @staticmethod
    def ssh_execute(
        command: str,
        ssh_hook: SSHHook,
    ) -> str:
        """Execute the given `command` via the `ssh_hook`."""
        exit_status, agg_stdout, agg_stderr = ssh_hook.exec_ssh_client_command(
            ssh_hook.get_conn(),
            command,
            timeout=60,
            get_pty=False,
            environment={},
        )
        logging.info(f"Got {exit_status=} {agg_stdout=} {agg_stderr=}")
        return agg_stdout.decode("utf-8").strip()


class QuantingMonitorOperator(SSHSensorOperator):
    """Monitor the status of a quanting job on the SLURM cluster."""

    @property
    def command_template(self) -> str:
        """See docu of superclass."""
        return """
    ST=$(sacct -j REPLACE_JID -o State | awk 'FNR == 3 {print $1}')
    echo $ST
    """

    @property
    def running_states(self) -> list[str]:
        """States that are considered 'running'."""
        return ["PENDING", "RUNNING"]
