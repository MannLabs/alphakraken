"""Airflow plugin that provides priority weight strategies for tasks.

See https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/priority-weight.html
"""

from datetime import datetime

import pytz
from airflow.plugins_manager import AirflowPlugin
from airflow.task.priority_strategy import PriorityWeightStrategy


class EpochPriorityStrategy(PriorityWeightStrategy):
    """A priority weight strategy that gives increasing weight with time."""

    def get_weight(self, *args, **kwargs) -> int:
        """Return the current epoch time (minutes) as the priority weight.

        Use minutes and baseline to avoid NumericValueOutOfRange error.
        """
        del args, kwargs  # unused

        current_epoch_time = datetime.now(tz=pytz.utc).timestamp()
        baseline = datetime(2024, 1, 1, tzinfo=pytz.utc).timestamp()

        return int((current_epoch_time - baseline) // 60)


class EpochPriorityWeightStrategyPlugin(AirflowPlugin):
    """Airflow plugin that provides the EpochPriorityStrategy."""

    name = "epoch_priority_weight_strategy_plugin"
    priority_weight_strategies = [EpochPriorityStrategy]  # noqa: RUF012
