"""S3 download trigger for the overview page."""

import os

import pandas as pd
import requests
import streamlit as st
import streamlit.delta_generator
from service.utils import BASELINE_PREFIX, _log

_DAG_ID = "s3_downloader"
_DAG_PARAM_RAW_FILE_IDS = "raw_file_ids"
_ENV_AIRFLOW_API_URL = "AIRFLOW_API_URL"
_ENV_AIRFLOW_USER = "AIRFLOW_USER"
_ENV_AIRFLOW_PASSWORD = "AIRFLOW_PASSWORD"  # noqa: S105
_REQUEST_TIMEOUT_S = 10
_MAX_PREVIEW_IDS = 20


def get_raw_file_ids_excluding_baselines(filtered_df: pd.DataFrame) -> list[str]:
    """Return raw file IDs from the DataFrame index, excluding baseline entries."""
    return [
        str(idx)
        for idx in filtered_df.index
        if not str(idx).startswith(BASELINE_PREFIX)
    ]


def trigger_s3_download(raw_file_ids: list[str]) -> tuple[bool, str]:  # noqa: PLR0911
    """Trigger the s3_downloader DAG via the Airflow REST API."""
    airflow_url = os.environ.get(_ENV_AIRFLOW_API_URL)
    airflow_user = os.environ.get(_ENV_AIRFLOW_USER)
    airflow_password = os.environ.get(_ENV_AIRFLOW_PASSWORD)

    if not airflow_url:
        return (
            False,
            f"Airflow API URL not configured. Set the {_ENV_AIRFLOW_API_URL} environment variable.",
        )

    if not airflow_user or not airflow_password:
        return (
            False,
            "Airflow credentials not configured. Set AIRFLOW_USER and AIRFLOW_PASSWORD environment variables.",
        )

    url = f"{airflow_url.rstrip('/')}/api/v1/dags/{_DAG_ID}/dagRuns"
    payload = {"conf": {_DAG_PARAM_RAW_FILE_IDS: ",".join(raw_file_ids)}}

    try:
        response = requests.post(
            url,
            json=payload,
            auth=(airflow_user, airflow_password),
            timeout=_REQUEST_TIMEOUT_S,
        )
    except requests.ConnectionError:
        return (
            False,
            f"Cannot connect to Airflow at {airflow_url}. Is the Airflow webserver running?",
        )
    except requests.Timeout:
        return (
            False,
            f"Request to Airflow timed out after {_REQUEST_TIMEOUT_S} seconds.",
        )

    if response.status_code == 200:  # noqa: PLR2004
        dag_run_id = response.json().get("dag_run_id", "unknown")
        return True, f"DAG run triggered successfully. Run ID: {dag_run_id}"

    if response.status_code in (401, 403):
        return False, "Authentication failed. Check AIRFLOW_USER and AIRFLOW_PASSWORD."

    if response.status_code == 404:  # noqa: PLR2004
        return False, f"DAG '{_DAG_ID}' not found. Is it deployed and unpaused?"

    if response.status_code == 409:  # noqa: PLR2004
        return False, "A DAG run conflict occurred. A run may already be in progress."

    return False, f"Airflow API error ({response.status_code}): {response.text}"


@st.dialog("Confirm S3 Download")
def _confirm_s3_download_dialog(raw_file_ids: list[str]) -> None:
    """Show a confirmation dialog before triggering the S3 download."""
    st.write(f"Trigger S3 download for **{len(raw_file_ids)}** files?")

    preview = raw_file_ids[:_MAX_PREVIEW_IDS]
    preview_text = "\n".join(preview)
    if len(raw_file_ids) > _MAX_PREVIEW_IDS:
        preview_text += f"\n... and {len(raw_file_ids) - _MAX_PREVIEW_IDS} more"
    st.code(preview_text)

    if st.button("Confirm Download"):
        with st.spinner("Triggering DAG ..."):
            success, message = trigger_s3_download(raw_file_ids)

        if success:
            st.success(message)
            _log(f"S3 download triggered for {len(raw_file_ids)} files.")
        else:
            st.error(message)
            _log(f"S3 download trigger failed: {message}")


def show_s3_download_button(
    filtered_df: pd.DataFrame, st_column: st.delta_generator.DeltaGenerator
) -> None:
    """Render the S3 download button and handle its click."""
    raw_file_ids = get_raw_file_ids_excluding_baselines(filtered_df)
    has_ids = len(raw_file_ids) > 0

    if st_column.button(
        "☁️ S3 Download",
        disabled=not has_ids,
        help="Trigger Airflow to download the selected files from S3."
        if has_ids
        else "No non-baseline files in selection.",
    ):
        _confirm_s3_download_dialog(raw_file_ids)
