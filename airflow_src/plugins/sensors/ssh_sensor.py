"""SSH sensor operator."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from airflow.sensors.base import BaseSensorOperator
from cluster_scripts.slurm_commands import get_job_handler
from common.keys import JobStates, XComKeys
from common.utils import (
    get_cluster_ssh_hook,
    get_xcom,
)

if TYPE_CHECKING:
    from airflow.providers.ssh.hooks.ssh import SSHHook


class QuantingSensorOperator(BaseSensorOperator, ABC):
    """Base class for sensor operators that watch over certain status of quanting."""

    @property
    @abstractmethod
    def command(self) -> str:
        """The command to execute."""

    @property
    @abstractmethod
    def states(self) -> list[str]:
        """Outputs of the command in `command_template` that are considered 'running'."""

    def __init__(self, *args, **kwargs) -> None:
        """Initialize the operator."""
        super().__init__(*args, **kwargs)
        self._job_id: str | None = None

    def pre_execute(self, context: dict[str, Any]) -> None:
        """_job_id the job id from XCom."""
        self._job_id = str(get_xcom(context["ti"], XComKeys.JOB_ID))


class SSHSensorOperator(QuantingSensorOperator, ABC):
    """Wait for a ssh command to return a certain output."""

    def __init__(self, *args, **kwargs) -> None:
        """Initialize the operator."""
        super().__init__(*args, **kwargs)
        self._ssh_hook: SSHHook = get_cluster_ssh_hook()

    def poke(self, context: dict[str, Any]) -> bool:
        """Check the output of the ssh command."""
        del context  # unused

        ssh_return = self.command  # ssh_execute(self.command, self._ssh_hook)
        logging.info(f"ssh command returned: '{ssh_return}'")

        return ssh_return not in self.states

    # show file size earlier


class WaitForJobStartSlurmSSHSensor(SSHSensorOperator):
    """Wait until a SLURM job leaves status 'PENDING'."""

    @property
    def command(self) -> str:
        """The command to execute.

        Must be a bash script that is executable on the cluster.
        Its only output to stdout must be the status of the queried job.
        """
        return get_job_handler().get_job_status(self._job_id)

    @property
    def states(self) -> list[str]:
        """List of states that keep the sensor waiting."""
        return [JobStates.PENDING]


class WaitForJobFinishSlurmSSHSensor(SSHSensorOperator):
    """Wait until a SLURM job leaves status 'RUNNING'."""

    @property
    def command(self) -> str:
        """The command to execute.

        Must be a bash script that is executable on the cluster.
        Its only output to stdout must be the status of the queried job.
        """
        return get_job_handler().get_job_status(self._job_id)

    @property
    def states(self) -> list[str]:
        """List of states that keep the sensor waiting."""
        return [JobStates.RUNNING]
