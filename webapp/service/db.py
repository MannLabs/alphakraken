"""Database service for the web application."""

import re
from datetime import datetime, timedelta

# ruff: noqa: PD002 # `inplace=True` should be avoided; it has inconsistent behavior
import pandas as pd
import pytz
import streamlit as st
from mongoengine import Q, QuerySet
from service.utils import _log

from shared.db.engine import connect_db
from shared.db.models import KrakenStatus, Metrics, Project, RawFile, Settings
from shared.keys import ALLOWED_CHARACTERS_PRETTY, FORBIDDEN_CHARACTERS_REGEXP


def get_raw_files_for_status_df(
    max_age_in_days: float,
) -> pd.DataFrame:
    """Get a DataFrame optimized for status display: only minimal set of information is extracted.

    :param max_age_in_days: Only consider files younger than this
    :return: DataFrame with all non-terminal entries, sorted by creation date
    """
    connect_db()
    min_created_at = pd.Timestamp(
        datetime.now(tz=pytz.UTC) - timedelta(days=max_age_in_days)
    )

    raw_files_db = (
        RawFile.objects(
            created_at__gte=min_created_at,
        )
        .only(
            "created_at",
            "updated_at_",
            "instrument_id",
            "status",
            "status_details",
        )
        .order_by("-created_at")
    )

    # Convert to DataFrame with proper column order
    df = pd.DataFrame(
        [
            {
                "instrument_id": r.instrument_id,
                "created_at": r.created_at,
                "updated_at_": r.updated_at_,
                "status": r.status,
                "status_details": r.status_details,
            }
            for r in raw_files_db
        ]
    )

    if len(df) == 0:
        return df

    # Ensure consistent order and reset index
    df.sort_values(
        ["instrument_id", "created_at"], ascending=[True, False], inplace=True
    )
    df.reset_index(drop=True, inplace=True)

    return df


def _validate_input(values: list[str] | None, param_name: str) -> None:
    """Validate that all values in the list contain only letters and numbers."""
    if not values:
        return

    for value in values:
        # FORBIDDEN_CHARACTERS_IN_RAW_FILE_NAME serves well here
        if re.match(FORBIDDEN_CHARACTERS_REGEXP, value):
            raise ValueError(
                f"Invalid parameter '{param_name}': '{value}' contains forbidden characters. Allowed: `!{ALLOWED_CHARACTERS_PRETTY}`"
            )


# Cached values are accessible to all users across all sessions.
# Considering memory it should currently be fine to have all data cached.
# Command for clearing the cache:  get_all_data.clear()
@st.cache_data(ttl=120)
def get_raw_file_and_metrics_data(
    max_age_in_days: float | None,
    raw_file_ids: list[str] | None,
    instruments: list[str] | None = None,
) -> tuple[QuerySet, QuerySet, datetime]:
    """Return from the database the QuerySets for RawFile and Metrics for files younger than max_age_in_days or for given list of raw file ids."""
    _validate_input(raw_file_ids, "raw_file_ids")
    _validate_input(instruments, "instruments")  # TODO: use query params accessor
    # max_age_in_days is implicitly validated to be numeric by converting it to timedelta

    if max_age_in_days is None and raw_file_ids is None:
        raise ValueError("Either max_age_in_days or raw_file_ids must be provided.")

    _log("Connecting to the database")
    connect_db()
    _log(
        f"Retrieving raw file and metrics {max_age_in_days=} {raw_file_ids=} {instruments=}"
    )

    if raw_file_ids is not None:
        # selection by raw file ids takes precedence over max_age_in_days and instruments
        q = Q(id__in=raw_file_ids)
    else:
        q = Q()
        if max_age_in_days is not None:
            min_created_at = pd.Timestamp(
                datetime.now(tz=pytz.UTC) - timedelta(days=max_age_in_days)
            )
            q &= Q(
                created_at__gt=min_created_at
            )  # query on file creation date ('created_at')
        if instruments is not None:
            q &= Q(instrument_id__in=instruments)

    raw_files_db = (
        RawFile.objects(q)
        # exclude some not-needed fields
        .exclude("file_info")
        .exclude("backup_base_path")
    )

    metrics_db = Metrics.objects(raw_file__in=raw_files_db)

    now = datetime.now(tz=pytz.UTC).replace(microsecond=0)

    _log(
        f"Done retrieving raw file and metrics {max_age_in_days=} {raw_file_ids=} {instruments=}"
    )

    return raw_files_db, metrics_db, now


def get_full_raw_file_data(raw_file_ids: list[str]) -> pd.DataFrame:
    """Return from the database a dataframe derived from the QuerySet for RawFile for all `raw_file_ids`."""
    _log("Connecting to the database")
    connect_db()
    _log(f"Retrieving all raw file data for {raw_file_ids}")

    raw_files_db = RawFile.objects.filter(id__in=raw_file_ids)

    _log(f"Done retrieving all raw file data for {raw_file_ids}")

    return df_from_db_data(raw_files_db)


def get_status_data() -> QuerySet:
    """Connect to the database and return the QuerySets for KrakenStatus."""
    _log("Connecting to the database")
    connect_db()
    _log("Retrieving all status data")
    objects = KrakenStatus.objects
    _log("Done retrieving all status data")

    return objects


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
    filter_dict: dict | None = None,
    drop_duplicates: list[str] | None = None,
    drop_columns: list[str] | None = None,
    drop_none_columns: bool = False,
) -> pd.DataFrame:
    """Create a DataFrame from a database QuerySet.

    :param query_set: the MongoDB QuerySet to convert to a DataFrame
    :param filter_dict: optional dictionary to filter the DataFrame, e.g {"status": "done"}
    :param drop_duplicates: optional list of columns to drop duplicates on after applying the filter
        (youngest created_at_ will be kept)
    :param drop_columns: optional list of columns to drop
    :param drop_none_columns: if True, drop columns that contain only None values
    :return: dataframe containing the data from the QuerySet
    """
    query_set_as_dicts = [r.to_mongo() for r in query_set]
    query_set_df = pd.DataFrame(query_set_as_dicts)

    if filter_dict:
        for key, value in filter_dict.items():
            if key not in query_set_df.columns:
                _log(
                    f"Warning: Key '{key}' not found in DataFrame columns '{query_set_df.columns}'."
                )
                continue
            query_set_df = query_set_df[query_set_df[key] == value]

    if len(query_set_df) == 0:
        return query_set_df

    if "created_at_" in query_set_df.columns:
        query_set_df.sort_values(by="created_at_", inplace=True, ascending=False)

    if drop_duplicates:
        query_set_df.drop_duplicates(subset=drop_duplicates, keep="first", inplace=True)

    if drop_columns:
        query_set_df.drop(
            columns=drop_columns,
            inplace=True,
            errors="ignore",
        )

    if drop_none_columns:
        # this is required for the Metrics, as e.g. custom metrics may not have all columns
        none_columns = [
            col for col in query_set_df.columns if query_set_df[col].isna().all()
        ]
        if none_columns:
            query_set_df.drop(
                columns=none_columns,
                inplace=True,
                errors="ignore",
            )

    query_set_df.reset_index(drop=True, inplace=True)

    return query_set_df
