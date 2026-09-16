"""Unit tests for the 'keys' module."""

from plugins.common.keys import Tasks


def test_tasks_all_values_returns_only_task_ids() -> None:
    """Test that all_values yields the task id constants and nothing else."""
    values = Tasks.all_values()

    assert Tasks.COMPUTE_CHECKSUM in values
    assert Tasks.GET_RAW_FILES_TO_REMOVE in values
    assert all(isinstance(value, str) for value in values)
    assert len(values) == len(set(values))
