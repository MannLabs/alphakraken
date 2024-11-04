"""Module handling the merging of raw files and metrics data."""

import pandas as pd
import streamlit as st
from service.db import df_from_db_data, get_raw_file_and_metrics_data


def get_combined_raw_files_and_metrics_df(max_age_in_days: float) -> pd.DataFrame:
    """Get the combined DataFrame of raw files and metrics."""
    raw_files_db, metrics_db = get_raw_file_and_metrics_data(max_age_in_days)
    raw_files_df = df_from_db_data(raw_files_db)
    metrics_df = df_from_db_data(
        metrics_db,
        drop_duplicates=["raw_file"],
        drop_columns=["_id", "created_at_"],
    )

    if len(raw_files_df) == 0 or len(metrics_df) == 0:
        st.write(f"No enough data yet: {len(raw_files_df)=} {len(metrics_df)=}.")
        st.dataframe(raw_files_df)
        st.dataframe(metrics_df)
        st.stop()

    # the joining could also be done on DB level
    combined_df = raw_files_df.merge(
        metrics_df, left_on="_id", right_on="raw_file", how="left"
    )

    # conversions
    combined_df["size_gb"] = combined_df["size"] / 1024**3
    combined_df["file_created"] = combined_df["created_at"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    combined_df["quanting_time_minutes"] = combined_df["quanting_time_elapsed"] / 60
    combined_df["precursors"] = combined_df["precursors"].astype(
        "Int64", errors="ignore"
    )
    combined_df["proteins"] = combined_df["proteins"].astype("Int64", errors="ignore")
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

    return combined_df
