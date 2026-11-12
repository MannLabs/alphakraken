"""Database service for the web application."""

import re
from datetime import datetime, timedelta

# ruff: noqa: PD002 # `inplace=True` should be avoided; it has inconsistent behavior
import pandas as pd
import pytz
from service.timezone import display_now, to_display_timezone
from service.utils import _log

from shared.db import metrics as metrics_store
from shared.db import raw_files as raw_file_store
from shared.display_paths import get_display_backup_path, get_display_output_path
from shared.domain.metrics import Metrics
from shared.domain.raw_files import TERMINAL_STATUSES, RawFile
from shared.validation import (
    ALLOWED_RAW_FILE_NAME_CHARACTERS_PRETTY,
    FORBIDDEN_RAW_FILE_NAME_CHARACTERS_PATTERN,
)


def get_raw_files_for_status_df(
    max_age_in_days: float,
) -> pd.DataFrame:
    """Get a DataFrame optimized for status display: only minimal set of information is extracted.

    :param max_age_in_days: Only consider files younger than this
    :return: DataFrame with all non-terminal entries, sorted by creation date
    """
    min_created_at = datetime.now(tz=pytz.UTC) - timedelta(days=max_age_in_days)

    raw_files_db = raw_file_store.find(
        raw_file_store.RawFileFilter(created_after=min_created_at)
    )

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

    df = to_display_timezone(df)

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
        if re.match(FORBIDDEN_RAW_FILE_NAME_CHARACTERS_PATTERN, value):
            raise ValueError(
                f"Invalid parameter '{param_name}': '{value}' contains forbidden characters. Allowed: `!{ALLOWED_RAW_FILE_NAME_CHARACTERS_PRETTY}`"
            )


def get_raw_file_and_metrics_data(
    max_age_in_days: float | None,
    raw_file_ids: list[str] | None,
    instruments: list[str] | None = None,
) -> tuple[list[RawFile], dict[str, dict[str, Metrics]], datetime]:
    """Return the raw files younger than max_age_in_days or with the given ids, and the latest metrics per raw file id and type.

    The metrics may cover more raw files than the returned ones.
    """
    # need to validate inputs as these values can be set by users
    _validate_input(raw_file_ids, "raw_file_ids")
    _validate_input(instruments, "instruments")  # TODO: use query params accessor
    # max_age_in_days is implicitly validated to be numeric by converting it to timedelta

    _log(
        f"Retrieving raw file and metrics {max_age_in_days=} {raw_file_ids=} {instruments=}"
    )

    if raw_file_ids is not None:
        # selection by raw file ids takes precedence over max_age_in_days and instruments
        found = raw_file_store.find(raw_file_store.RawFileFilter(ids=raw_file_ids))
        latest_metrics = metrics_store.latest_by_raw_file(raw_file_ids)
    elif max_age_in_days is None:
        raise ValueError("Either max_age_in_days or raw_file_ids must be provided.")
    else:
        min_created_at = datetime.now(tz=pytz.UTC) - timedelta(days=max_age_in_days)
        found = raw_file_store.find(
            raw_file_store.RawFileFilter(
                created_after=min_created_at, instrument_ids=instruments
            )
        )
        # Metrics can only be written after their raw file was registered, so restricting
        # them by their own insertion date yields a superset of the metrics of the selected
        # raw files. Selecting them by raw file id instead would mean sending one id per
        # selected raw file to the database. The superset is discarded when joining, cf.
        # get_combined_raw_files_and_metrics_df().
        latest_metrics = metrics_store.latest_by_raw_file(created_after=min_created_at)

    now = display_now().replace(microsecond=0)

    _log(
        f"Done retrieving raw file and metrics {max_age_in_days=} {raw_file_ids=} {instruments=}"
    )

    return found, latest_metrics, now


def get_full_raw_file_data(raw_file_ids: list[str]) -> pd.DataFrame:
    """Return a dataframe of the raw files with the given ids, including their file info.

    The column `backup_path` holds the folder each raw file is backed up to, as users see it.
    """
    _log(f"Retrieving all raw file data for {raw_file_ids}")

    found = raw_file_store.find(
        raw_file_store.RawFileFilter(ids=raw_file_ids), include_file_info=True
    )

    _log(f"Done retrieving all raw file data for {raw_file_ids}")

    df = df_from_db_data(found)
    if len(df) == 0:
        return df
    # by id: the dataframe is sorted differently than the query result
    backup_paths = {
        raw_file.id: get_display_backup_path(raw_file) for raw_file in found
    }
    df["backup_path"] = df["id"].map(backup_paths)
    return df


def get_output_folders(raw_file_ids: list[str]) -> pd.DataFrame:
    """Return a DataFrame of output folders for the given raw file ids.

    Because of the N:1 relation between metrics and raw files (multiple settings and/or
    types can be run for one raw file), a single raw file may map to several output folders.
    """
    _log(f"Retrieving output folders for {raw_file_ids}")

    # no values needed, only the metadata
    all_metrics = metrics_store.find(raw_file_ids, fields=[])

    rows = [
        {
            "raw_file_id": m.raw_file_id,
            "settings_name": m.settings_name,
            "settings_version": m.settings_version,
            "type": m.type,
            "output_path": None
            if m.relative_output_path is None
            else get_display_output_path(
                m.relative_output_path, m.output_display_base_path
            ),
        }
        for m in all_metrics
    ]

    _log(f"Done retrieving output folders for {raw_file_ids}")

    return pd.DataFrame(
        rows,
        columns=[
            "raw_file_id",
            "settings_name",
            "settings_version",
            "type",
            "output_path",
        ],
    )


def get_raw_files_for_throughput_per_day(days: int = 14) -> pd.DataFrame:
    """Get a DataFrame with sample count and data volume per day, grouped by instrument.

    :param days: Number of days to look back (default: 14)
    :return: DataFrame with columns: date, instrument_id, count, size_gb
    """
    min_created_at = datetime.now(tz=pytz.UTC) - timedelta(days=days)

    raw_files_db = raw_file_store.find(
        raw_file_store.RawFileFilter(
            created_after=min_created_at, statuses=TERMINAL_STATUSES
        )
    )

    if not raw_files_db:
        return pd.DataFrame(columns=["date", "instrument_id", "count", "size_gb"])

    df = pd.DataFrame(
        [
            {
                "instrument_id": r.instrument_id,
                "created_at": r.created_at,
                "size": r.size if r.size and r.size > 0 else 0,
            }
            for r in raw_files_db
        ]
    )

    # df = to_display_timezone(df) # no timezone conversion needed: we only care about the date, not the time
    df["date"] = df["created_at"].dt.date

    result = (
        df.groupby(["date", "instrument_id"])
        .agg(
            count=("instrument_id", "size"),
            size=("size", "sum"),
        )
        .reset_index()
    )
    result["size_gb"] = result["size"] / (1024**3)
    return result[["date", "instrument_id", "count", "size_gb"]]


def df_from_db_data(
    rows: list,
    *,
    drop_none_columns: bool = False,
) -> pd.DataFrame:
    """Create a DataFrame from database results.

    :param rows: the domain objects to convert to a DataFrame, one row each
    :param drop_none_columns: if True, drop columns that contain only None values
    :return: dataframe with one column per field of the domain objects
    """
    query_set_df = pd.DataFrame([vars(row) for row in rows])

    if "created_at_" in query_set_df.columns:
        query_set_df.sort_values(by="created_at_", inplace=True, ascending=False)

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

    return to_display_timezone(query_set_df)
