"""Module handling the merging of raw files and metrics data."""

from datetime import datetime

import pandas as pd
import streamlit as st
from service.columns import build_alternative_names_mapping, load_columns_from_yaml
from service.db import df_from_db_data, get_raw_file_and_metrics_data
from service.utils import Cols

from shared.db.models import RawFileStatus
from shared.keys import MetricsTypes

_ALTERNATIVE_NAMES_MAPPING = build_alternative_names_mapping(load_columns_from_yaml())


def _normalize_metric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Resolve alternative DB column names to canonical names and apply data transforms."""
    df = df.rename(columns=_ALTERNATIVE_NAMES_MAPPING)

    # deduplicate columns that map to the same canonical name
    df = df.groupby(axis=1, level=0).first()

    if "gradient_length" in df.columns:
        df["gradient_length"] = df["gradient_length"].apply(lambda x: round(x, 1))

    return df


def get_combined_raw_files_and_metrics_df(
    *,
    max_age_in_days: float | None = None,
    raw_file_ids: list[str] | None = None,
    print_at_no_data: bool = False,
    instruments: list[str] | None = None,
) -> tuple[pd.DataFrame, datetime]:
    """Get the combined DataFrame of raw files and metrics."""
    raw_files_db, metrics_db, data_timestamp = get_raw_file_and_metrics_data(
        max_age_in_days, raw_file_ids, instruments
    )
    raw_files_df = df_from_db_data(raw_files_db)

    # merge metrics of each type into a single DataFrame
    # TODO: first existing metrics gets column name w/o suffix -> always append
    metrics_types = [
        MetricsTypes.ALPHADIA,
        MetricsTypes.CUSTOM,
        MetricsTypes.MSQC,
        MetricsTypes.SKYLINE,
        MetricsTypes.DIANN,
    ]
    merged_metrics_df = pd.DataFrame()
    for metrics_type in metrics_types:
        metrics_df = df_from_db_data(
            metrics_db,
            filter_dict={"type": metrics_type},
            drop_duplicates=["raw_file"],
            drop_columns=["_id", "created_at_"],
            drop_none_columns=True,
        )
        if len(metrics_df) == 0:
            continue

        metrics_df = _normalize_metric_columns(metrics_df)

        # prefix all metric columns with "{type}__"
        metrics_df = metrics_df.drop(columns=["type"], errors="ignore")
        metric_cols = [c for c in metrics_df.columns if c != "raw_file"]
        metrics_df = metrics_df.rename(
            columns={c: f"{metrics_type}__{c}" for c in metric_cols}
        )

        if len(merged_metrics_df) == 0:
            merged_metrics_df = metrics_df
        else:
            merged_metrics_df = merged_metrics_df.merge(
                metrics_df,
                on="raw_file",
                how="outer",
            )

    if len(raw_files_df) == 0:
        # TODO: improve -> move st dependency out
        if print_at_no_data:
            # just for debugging
            st.write(f"[{len(raw_files_df)=} {len(merged_metrics_df)=}]")
            st.dataframe(raw_files_df)
            st.dataframe(merged_metrics_df)
        return pd.DataFrame(), data_timestamp

    if len(merged_metrics_df) > 0:
        combined_df = raw_files_df.merge(
            merged_metrics_df, left_on="_id", right_on="raw_file", how="left"
        )
    else:
        combined_df = raw_files_df

    # conversions
    combined_df["size_gb"] = combined_df["size"] / 1024**3
    del combined_df["size"]

    combined_df["file_created"] = combined_df["created_at"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    combined_df["created_at"] = combined_df["created_at"].apply(
        lambda x: x.replace(microsecond=0)
    )
    combined_df["updated_at_"] = combined_df["updated_at_"].apply(
        lambda x: x.replace(microsecond=0)
    )
    combined_df["created_at_"] = combined_df["created_at_"].apply(
        lambda x: x.replace(microsecond=0)
    )
    # sorting & indexing
    combined_df.sort_values(by="created_at", ascending=False, inplace=True)  # noqa: PD002
    combined_df.reset_index(drop=True, inplace=True)  # noqa: PD002
    combined_df.index = combined_df["_id"]

    # conversion of metrics columns: in case all quantings have failed, these columns are not available
    for col in [
        c for c in combined_df.columns if c.endswith("__quanting_time_elapsed")
    ]:
        prefix = col.rsplit("__", 1)[0]
        combined_df[f"{prefix}__quanting_time_minutes"] = combined_df[col] / 60
        del combined_df[col]

    for col in [
        c for c in combined_df.columns if c.endswith(("__precursors", "__proteins"))
    ]:
        combined_df[col] = combined_df[col].astype("Int64", errors="ignore")

    combined_df[Cols.IS_BASELINE] = False

    return combined_df, data_timestamp


def get_lag_time(
    raw_files_df: pd.DataFrame, num_files: int = 10
) -> tuple[pd.Series | None, float | None]:
    """Get the average lag time in minutes for the latest N files with 'done' status.

    Lag time is calculated as the difference between updated_at_
        and created_at_ (= when the file was added to the DB).
    Deliberately not using the file_created column, as this depends on the instrument time.

    :return: lag_times, and average lag time in seconds, or (None, None) if no done files found
    """
    done_files = raw_files_df[raw_files_df["status"] == RawFileStatus.DONE]

    if len(done_files) == 0:
        return None, None

    lag_times = (
        done_files["updated_at_"] - done_files["created_at_"]
    ).dt.total_seconds()

    # Sort by created_at_ (most recent first) and take the latest N files
    done_files = done_files.sort_values(by="created_at_", ascending=False).head(
        min(num_files, len(done_files))
    )

    # Calculate lag time in minutes
    mean_last_10_lag_times = (
        (done_files["updated_at_"] - done_files["created_at_"])
        .dt.total_seconds()
        .mean()
    )

    return lag_times, mean_last_10_lag_times
