"""Airflow plugin that provides priority weight strategies for tasks.

See https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/priority-weight.html
"""

import time

from airflow.plugins_manager import AirflowPlugin
from airflow.task.priority_strategy import PriorityWeightStrategy


class EpochPriorityStrategy(PriorityWeightStrategy):
    """A priority weight strategy that gives increasing weight with time."""

    def get_weight(self, *args, **kwargs) -> int:
        """Return the current epoch time (seconds) as the priority weight."""
        del args, kwargs  # unused
        return int(time.time())


class EpochPriorityWeightStrategyPlugin(AirflowPlugin):
    """Airflow plugin that provides the EpochPriorityStrategy."""

    name = "epoch_priority_weight_strategy_plugin"
    priority_weight_strategies = [EpochPriorityStrategy]  # noqa: RUF012
