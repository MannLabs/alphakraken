"""Tests that the input types and the Mongo models declare the same limits."""

import pytest

from shared.db._mongo import models
from shared.domain.alerts import NewSuppression
from shared.domain.inputs import InputModel, max_length
from shared.domain.projects import NewProject
from shared.domain.settings import NewSettings
from shared.domain.users import NewUser

# model fields named differently than the field of the input type
_MODEL_FIELD_NAMES = {(NewProject, "project_id"): "id"}


@pytest.mark.parametrize(
    ("input_type", "model", "fields"),
    [
        (NewUser, models.User, ["email", "initials", "messenger_id"]),
        (NewProject, models.Project, ["project_id", "name", "description"]),
        (
            NewSettings,
            models.Settings,
            [
                "name",
                "description",
                "fasta_file_name",
                "speclib_file_name",
                "config_file_name",
                "config_params",
                "software",
                "runner_name",
                "slurm_mem",
                "slurm_time",
            ],
        ),
        (
            NewSuppression,
            models.AlertSuppression,
            [
                "alert",
                "instrument_id",
                "identifier",
                "details",
                "comment",
                "created_by",
            ],
        ),
    ],
)
def test_input_type_and_model_declare_the_same_max_length(
    input_type: type[InputModel], model: type, fields: list[str]
) -> None:
    """A value that passes the input type is not rejected by the model, and vice versa."""
    for field in fields:
        model_field = getattr(model, _MODEL_FIELD_NAMES.get((input_type, field), field))

        assert max_length(input_type, field) == model_field.max_length, field
