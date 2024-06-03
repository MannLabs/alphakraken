"""Test the DB wrapper."""

from unittest.mock import MagicMock, patch

# ruff: noqa: SLF001 # Private member accessed
import pytest

from shared.db.models import RawFile
from shared.db.wrapper import MongoDBWrapper

SOME_TABLE = "raw_files"


@pytest.fixture()
@patch("shared.db.wrapper.MongoClient")
def wrapper(mock_client: MagicMock) -> MongoDBWrapper:
    """Return a MongoDBClient instance with a mocked table."""
    mock_table = MagicMock()
    d = {"krakendb": {SOME_TABLE: mock_table}}

    mock_client.return_value.__getitem__.side_effect = d.__getitem__

    return MongoDBWrapper()


@pytest.fixture()
def item() -> RawFile:
    """Return a RawFile instance. Note that this implicitly tests also the models.py module."""
    return RawFile("some_name")


def test_insert_new_item(wrapper: MagicMock, item: MagicMock) -> None:
    """Inserting a new item works."""
    # given
    mock_table = wrapper._db[SOME_TABLE]
    mock_table.count_documents.return_value = 0
    mock_table.insert_one.return_value.inserted_id = "123"

    # when
    result = wrapper.insert(item)

    # then
    assert result == "123"
    mock_table.count_documents.assert_called_once_with(item.pk)
    mock_table.insert_one.assert_called_once_with(item.to_dict())


def test_insert_existing_item_raises_error(wrapper: MagicMock, item: MagicMock) -> None:
    """Inserting an existing item raises an error."""
    # given
    mock_table = wrapper._db[SOME_TABLE]
    mock_table.count_documents.return_value = 1

    # when & then
    with pytest.raises(ValueError):
        wrapper.insert(item)


def test_update_existing_item(wrapper: MagicMock, item: MagicMock) -> None:
    """Updating an existing item works."""
    # given
    mock_table = wrapper._db[SOME_TABLE]
    mock_table.update_one.return_value.matched_count = 1

    # when
    wrapper.update(item)

    # then
    mock_table.update_one.assert_called_once_with(item.pk, {"$set": item.to_dict()})


def test_update_non_existing_item_raises_error(
    wrapper: MagicMock, item: MagicMock
) -> None:
    """Updating a non-existing item raises an error."""
    # given
    mock_table = wrapper._db[SOME_TABLE]
    mock_table.update_one.return_value.matched_count = 0

    # when & then
    with pytest.raises(ValueError):
        wrapper.update(item)


def test_find_matching_items(wrapper: MagicMock, item: MagicMock) -> None:
    """Finding matching items works."""
    # given
    mock_table = wrapper._db[SOME_TABLE]
    mock_table.find.return_value = [item.to_dict()]

    # when
    result = wrapper.find(item)

    # then
    assert result == [item]
    mock_table.find.assert_called_once_with(item.to_dict())


def test_count_matching_items(wrapper: MagicMock, item: MagicMock) -> None:
    """Counting matching items works."""
    # given
    mock_table = wrapper._db[SOME_TABLE]
    mock_table.count_documents.return_value = 1

    # when
    result = wrapper.count(item)

    # then
    assert result == 1
    mock_table.count_documents.assert_called_once_with(item.to_dict())
