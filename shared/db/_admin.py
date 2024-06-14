"""Admin functions for the database.

These are not meant to be called by the code but by the developer/administrator from within a container
that has acces to the DB.

WARNING: these methods alter the database schema and should be used with caution.
Make sure all services that write to the DB are stopped before running these functions.
"""

import logging

from db.engine import DB_HOST, DB_NAME, DB_PASSWORD, DB_PORT, DB_USER


def _rename_field_in_collection(
    collection_name: str, from_name: str, to_name: str
) -> None:
    """Rename a field in a collection.

    e.g.
    _rename_field_in_collection("raw_file", "db_entry_created_at", "created_at_")
    """
    logging.info(f"Renaming in {collection_name} field {from_name} to {to_name}.")

    from pymongo import MongoClient

    client = MongoClient(
        f"mongodb://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}?authSource={DB_NAME}"
    )
    db = client[DB_NAME]

    collection = db[collection_name]

    ret_val = collection.update_many(
        {to_name: {"$exists": False}}, {"$rename": {from_name: to_name}}
    )

    logging.info(f"Got result {ret_val}.")
