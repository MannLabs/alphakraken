"""Database service for the web application."""

# ruff: noqa: PD002 # `inplace=True` should be avoided; it has inconsistent behavior
import pandas as pd
import streamlit as st
from mongoengine import QuerySet
from service.utils import _log

from shared.db.engine import Metrics, RawFile, connect_db


# Cached values are accessible to all users across all sessions.
# Considering memory it should currently be fine to have all data cached.
# Command for clearing the cache:  get_all_data.clear()
@st.cache_data(ttl=60)
def get_all_data() -> tuple[QuerySet, QuerySet]:
    """Connect to the database and return the QuerySets for RawFile and Metrics."""
    _log("Connecting to the database")
    connect_db()
    raw_files_db = RawFile.objects
    metrics_db = Metrics.objects
    return raw_files_db, metrics_db


def df_from_db_data(
    query_set: QuerySet,
    *,
    drop_duplicates: list[str] | None = None,
    drop_columns: list[str] | None = None,
) -> pd.DataFrame:
    """Create a DataFrame from a database QuerySet.

    :param query_set: the MongoDB QuerySet to convert to a DataFrame
    :param drop_duplicates: optional list of columns to drop duplicates on
    :param drop_columns: optional list of columns to drop
    :return: dataframe containing the data from the QuerySet
    """
    query_set_as_dicts = [r.to_mongo() for r in query_set]
    query_set_df = pd.DataFrame(query_set_as_dicts)

    if drop_duplicates:
        query_set_df.drop_duplicates(subset=drop_duplicates, keep="last", inplace=True)

    if drop_columns:
        query_set_df.drop(
            columns=drop_columns,
            inplace=True,
            errors="ignore",
        )
    return query_set_df
