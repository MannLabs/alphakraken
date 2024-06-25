"""Database service for the web application."""

# ruff: noqa: PD002 # `inplace=True` should be avoided; it has inconsistent behavior
import pandas as pd
import streamlit as st
from db.engine import connect_db
from mongoengine import QuerySet
from service.utils import _log

from shared.db.models import Metrics, Project, RawFile, Settings


# Cached values are accessible to all users across all sessions.
# Considering memory it should currently be fine to have all data cached.
# Command for clearing the cache:  get_all_data.clear()
@st.cache_data(ttl=60)
def get_raw_file_and_metrics_data() -> tuple[QuerySet, QuerySet]:
    """Connect to the database and return the QuerySets for RawFile and Metrics."""
    _log("Connecting to the database")
    connect_db()
    _log("Retrieving all raw file and metrics data")
    raw_files_db = RawFile.objects
    metrics_db = Metrics.objects

    return raw_files_db, metrics_db


def get_project_data() -> QuerySet:
    """Connect to the database and return the QuerySet for Project."""
    _log("Connecting to the database")
    connect_db()
    _log("Retrieving all project data")

    return Project.objects


def get_settings_data() -> QuerySet:
    """Connect to the database and return the QuerySet for Settings."""
    _log("Connecting to the database")
    connect_db()
    _log("Retrieving all settings data")

    return Settings.objects


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
    if len(query_set_df) == 0:
        return query_set_df

    query_set_df.sort_values(by="created_at_", inplace=True, ascending=False)

    if drop_duplicates:
        query_set_df.drop_duplicates(subset=drop_duplicates, keep="first", inplace=True)

    if drop_columns:
        query_set_df.drop(
            columns=drop_columns,
            inplace=True,
            errors="ignore",
        )

    query_set_df.reset_index(drop=True, inplace=True)

    return query_set_df
