"""Tests for the response models of the REST API."""

from rest_api.main import app
from rest_api.schemas import RawFileResponse, SettingsResponse
from shared.tests.factories import settings


def test_settings_response_of_legacy_settings() -> None:
    """Legacy settings, with values the domain type does not allow, can be served."""
    # the type hints of the domain do not hold for documents written before the validation
    legacy = settings(number=-1, runner_name=None, metrics_type=None, owner_email=None)

    response = SettingsResponse.model_validate(legacy)

    assert response.number == -1
    assert response.runner_name is None
    assert response.metrics_type is None


def test_openapi_lists_all_raw_file_fields() -> None:
    """The schema of a raw file has every field of the model, none are undeclared extras."""
    schema = app.openapi()["components"]["schemas"]["RawFileResponse"]

    assert set(schema["properties"]) == set(RawFileResponse.model_fields)
    assert schema.get("additionalProperties") is not True
    assert set(schema["properties"]) >= {
        "collision_flag",
        "status_details",
        "backup_status",
        "backup_display_base_path",
        "s3_upload_path",
        "instrument_file_status",
        "finalized_at",
    }
