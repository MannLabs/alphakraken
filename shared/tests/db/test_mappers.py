"""Tests for the mappers of the Mongo backend."""

from unittest.mock import patch

import pytest
from bson import DBRef, ObjectId

from shared.db._mongo import models
from shared.db._mongo.mappers import reference_id

SETTINGS_OBJECT_ID = ObjectId("65a0000000000000000000aa")


def _loaded_assignment() -> models.ProjectSettings:
    """An assignment as it comes from the database: mongoengine holds its references as `DBRef`s."""
    return models.ProjectSettings._from_son(
        {"_id": ObjectId(), "project": "P1", "settings": SETTINGS_OBJECT_ID}
    )


@pytest.mark.parametrize(
    ("field", "expected"), [("project", "P1"), ("settings", SETTINGS_OBJECT_ID)]
)
def test_reference_id_of_loaded_document(field: str, expected: object) -> None:
    """The id is read from the stored reference, for string and for ObjectId primary keys."""
    assert reference_id(_loaded_assignment(), field) == expected


def test_reference_id_does_not_query_or_serialize() -> None:
    """Reading the id neither dereferences the document nor serializes it."""
    document = _loaded_assignment()

    with (
        patch.object(models.ProjectSettings, "to_mongo") as mock_to_mongo,
        patch("mongoengine.fields.ReferenceField.__get__") as mock_dereference,
    ):
        assert reference_id(document, "project") == "P1"

    mock_to_mongo.assert_not_called()
    mock_dereference.assert_not_called()


def test_reference_id_of_loaded_document_with_legacy_dbref() -> None:
    """A reference stored as DBRef by an older version is read the same way."""
    document = models.Project._from_son(
        {"_id": "P1", "name": "n", "owner": DBRef("user", "a@b.de")}
    )

    assert reference_id(document, "owner") == "a@b.de"


def test_reference_id_of_document_built_in_memory() -> None:
    """On a document built in memory, e.g. right after an insert, the field holds the referenced document."""
    document = models.Project(
        id="P1", name="n", owner=models.User(email="a@b.de", initials="ab")
    )

    assert reference_id(document, "owner") == "a@b.de"


def test_reference_id_of_unset_reference() -> None:
    """An unset reference has no id."""
    document = models.Project._from_son({"_id": "P1", "name": "n"})

    assert reference_id(document, "owner") is None
