"""SSH sensor operator."""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any

from airflow.sensors.base import BaseSensorOperator
from common.keys import JobStates, XComKeys
from common.utils import (
    get_xcom,
)
from jobs.job_handler import get_job_handler


class QuantingSensorOperator(BaseSensorOperator, ABC):
    """Base class for sensor operators that watch over certain status of quanting."""

    @property
    def job_status(self) -> str:
        """The status of the quanting job."""
        return get_job_handler().get_job_status(self._job_id)

    @property
    @abstractmethod
    def states(self) -> list[str]:
        """Quanting job states that are considered 'running'."""

    def __init__(self, *args, **kwargs) -> None:
        """Initialize the operator."""
        super().__init__(*args, **kwargs)
        self._job_id: str | None = None

    def pre_execute(self, context: dict[str, Any]) -> None:
        """Persist the job id from XCom."""
        self._job_id = str(get_xcom(context["ti"], XComKeys.JOB_ID))

    def poke(self, context: dict[str, Any]) -> bool:
        """Check the output of the ssh command."""
        del context  # unused

        logging.info(f"job_status: '{self.job_status}'")

        return self.job_status not in self.states

    # show file size earlier


class WaitForJobStartSensor(QuantingSensorOperator):
    """Wait until a job leaves status 'PENDING'."""

    @property
    def states(self) -> list[str]:
        """List of states that keep the sensor waiting."""
        return [JobStates.PENDING]


class WaitForJobFinishSensor(QuantingSensorOperator):
    """Wait until a job leaves status 'RUNNING'."""

    @property
    def states(self) -> list[str]:
        """List of states that keep the sensor waiting."""
        return [JobStates.RUNNING]
