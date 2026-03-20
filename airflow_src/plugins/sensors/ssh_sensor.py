"""SSH sensor operator."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

from airflow.sensors.base import BaseSensorOperator
from airflow.utils.xcom import XCOM_RETURN_KEY
from common.keys import JobStates
from common.utils import (
    get_xcom,
)
from jobs.job_handler import get_job_status


class JobStatusSensorOperator(BaseSensorOperator, ABC):
    """Base class for sensor operators that watch over certain statuses of a job."""

    @property
    @abstractmethod
    def states(self) -> list[str]:
        """Job states that are considered 'running'."""

    def __init__(
        self,
        xcom_source_task_id: str,
        *args,
        **kwargs,
    ) -> None:
        """Initialize the operator.

        :param xcom_source_task_id: The task id of the task that pushes the job id to XCom.
        """
        super().__init__(*args, **kwargs)
        self.xcom_source_task_id: str = xcom_source_task_id
        self._job_id: str | None = None

    def pre_execute(self, context: dict[str, Any]) -> None:
        """Persist the job id from XCom."""
        self._job_id = str(
            get_xcom(context["ti"], XCOM_RETURN_KEY, task_ids=self.xcom_source_task_id)
        )

    def poke(self, context: dict[str, Any]) -> bool:
        """Check the output of the ssh command."""
        del context  # unused

        job_status = get_job_status(self._job_id)
        logging.info(f"job_status: '{job_status}'")

        return job_status not in self.states


class WaitForJobStartSensor(JobStatusSensorOperator):
    """Wait until a job leaves status 'PENDING'."""

    @property
    def states(self) -> list[str]:
        """List of states that keep the sensor waiting."""
        return [JobStates.PENDING]


class WaitForJobFinishSensor(JobStatusSensorOperator):
    """Wait until a job leaves status 'RUNNING'."""

    @property
    def states(self) -> list[str]:
        """List of states that keep the sensor waiting."""
        return [
            JobStates.PENDING,  # on the edge of hacky: in case we don't use a WaitForJobStartSensor, we need to wait for the job to start here
            JobStates.RUNNING,
        ]
