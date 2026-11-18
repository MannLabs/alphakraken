"""Tests that the write functions take every field of their input type."""

import inspect
from collections.abc import Callable
from typing import Any

import pytest

from shared.db import alerts, project_settings, projects, settings, users
from shared.domain.alerts import NewSuppression
from shared.domain.inputs import InputModel
from shared.domain.projects import NewAssignment, NewProject
from shared.domain.settings import NewSettings
from shared.domain.users import NewUser, UserUpdate


@pytest.mark.parametrize(
    ("write", "model", "not_fields"),
    [
        (alerts.add_suppression, NewSuppression, set()),
        (project_settings.assign, NewAssignment, {"allow_fallback"}),
        (projects.add, NewProject, {"allow_fallback"}),
        (settings.create, NewSettings, {"archive_previous"}),
        (users.add, NewUser, set()),
        (users.update, UserUpdate, {"email"}),
    ],
)
def test_write_function_takes_every_field(
    write: Callable[..., Any], model: type[InputModel], not_fields: set[str]
) -> None:
    """A field added to the input type must also be added to the write function."""
    parameters = inspect.signature(write).parameters.keys()

    assert parameters - not_fields == model.model_fields.keys()
