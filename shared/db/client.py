"""A client to interact with a MongoDB database.

Offers basic CRU operations.
"""

import logging
from typing import TYPE_CHECKING

from pymongo import MongoClient

if TYPE_CHECKING:
    from pymongo.results import UpdateResult

from shared.db.models import MongoBaseModel

DEFAULT_HOST = "mongodb://mongodb"

DEFAULT_DB_NAME = "krakendb"

DEFAULT_DB_PORT = 27017


class MongoDBClient:
    """A MongoDB client that provides methods to interact with a MongoDB database."""

    def __init__(self) -> None:
        """Initialize a new instance of the MongoDBClient class."""
        self.client = MongoClient(DEFAULT_HOST, DEFAULT_DB_PORT)
        self.db = self.client[DEFAULT_DB_NAME]

    def insert(self, item: MongoBaseModel) -> str:
        """Insert a new item into a collection.

        :param item: the item to insert
        :return: The _id of the inserted document.
        """
        logging.info(f"Inserting {item.to_dict()} into {item.table}")

        if self.db[item.table].count_documents(item.pk):
            raise ValueError(f"Item with pk {item.pk} already exists in {item.table}")

        inserted_id = str(self.db[item.table].insert_one(item.to_dict()).inserted_id)
        logging.info(f"Got id {inserted_id}")
        return inserted_id

    def update(self, item: MongoBaseModel) -> None:
        """Update an item in a collection.

        :param item: the item to insert
        :raises ValueError: If the item is not found in the collection.
        :return: The _id of the inserted document.
        """
        logging.info(f"Updating {item.to_dict()} in {item.table}")
        update_result: UpdateResult = self.db[item.table].update_one(
            item.pk, {"$set": item.to_dict()}
        )

        if update_result.matched_count == 0:
            raise ValueError(f"Item {item.to_dict()} not found in {item.table}")

    def find(self, item: MongoBaseModel) -> list[MongoBaseModel]:
        """Retrieve items from a collection that match an item.

        Beware: on breaking changes of the model, the old fields will be ignored.
            E.g.: model field was renamed from "a" to "b" -> model.b will be None for all old entries.

        :param item: The item to match documents against.
        :return: A list of matching documents.
        """
        logging.info(f"Finding {item.to_dict()} in {item.table}")
        return [item.from_dict(a) for a in self.db[item.table].find(item.to_dict())]

    def count(self, item: MongoBaseModel) -> int:
        """Count the number of documents in a collection that match an item.

        :param item: The item to match documents against.
        :return: The number of matching documents.
        """
        logging.info(f"Counting {item.to_dict()} in {item.table}")
        return self.db[item.table].count_documents(item.to_dict())
