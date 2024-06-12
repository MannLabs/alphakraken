"""SSH sensor operator."""

import logging
from abc import ABC, abstractmethod

from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.sensors.base import BaseSensorOperator
from common.keys import Variables, XComKeys
from common.settings import get_internal_output_path
from common.utils import get_variable, get_xcom


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
        self._ssh_hook = ssh_hook
        self._jid = None
        self._raw_file_name = None

    def pre_execute(self, context: dict[str, any]) -> None:
        """Get the job id and raw file name from XCom."""
        self._jid = get_xcom(context["ti"], XComKeys.JOB_ID)
        self._raw_file_name = get_xcom(context["ti"], XComKeys.RAW_FILE_NAME)

    def poke(self, context: dict[str, any]) -> bool:
        """Check the output of the ssh command."""
        logging.info(f"SSH command execute: {context}")

        command = self.command_template.replace("REPLACE_JID", self._jid)

        ssh_return = self.ssh_execute(command, self._ssh_hook)
        logging.info("SSH command returned: '%s'", ssh_return)

        log_file_path = get_internal_output_path(self._raw_file_name) / "log.txt"

        try:
            with open(log_file_path) as f:  # noqa: PTH123
                for line in f:
                    logging.info(line)
        except Exception as e:  # noqa:BLE001
            logging.warning(f"Exception when trying to read alphadia log: {e}.")

        try:
            slurm_output_file = f"~/kraken/slurm-{self._jid}.out"
            print_slurm_output_file_cmd = f"cat {slurm_output_file}"
            self.ssh_execute(print_slurm_output_file_cmd, self._ssh_hook)
        except Exception as e:  # noqa:BLE001
            logging.warning(f"Exception when trying to read slurm log: {e}.")

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
        if get_variable(Variables.DEBUG_NO_CLUSTER_SSH, "False") == "True":
            logging.warning(
                f"Variable {Variables.DEBUG_NO_CLUSTER_SSH} set: Not running SSH command {command} on cluster."
            )
            return "COMPLETED"

        exit_status, agg_stdout, agg_stderr = ssh_hook.exec_ssh_client_command(
            ssh_hook.get_conn(),
            command,
            timeout=60,
            get_pty=False,
            environment={},
        )
        logging.info(f"Got {exit_status=} {agg_stdout=} {agg_stderr=}")
        return agg_stdout.decode("utf-8").strip()


class QuantingSSHSensor(SSHSensorOperator):
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
