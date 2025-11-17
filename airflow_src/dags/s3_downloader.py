"""DAG to download raw files from S3 to output location."""

from __future__ import annotations

from datetime import timedelta

from airflow.models import Param
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from common.constants import AIRFLOW_QUEUE_PREFIX, Pools
from common.keys import DagParams, Dags
from impl.s3_download import download_raw_files_from_s3

# S3 Download DAG - Manual restoration of raw files from S3 to output location
with (
    DAG(
        Dags.S3_DOWNLOADER,
        schedule=None,  # Manual trigger only
        catchup=False,
        # Default arguments for each task
        default_args={
            "depends_on_past": False,
            "retries": 0,
            "retry_delay": timedelta(minutes=5),
            "queue": f"{AIRFLOW_QUEUE_PREFIX}s3_download",  # TODO: HERE: need dedicated worker
        },
        description="Download raw files from S3 to output location (batch mode supported)",
        tags=["manual"],
        params={
            DagParams.RAW_FILE_IDS: Param(
                type="string",
                minimum=3,
                description=(
                    "Comma-separated list of RawFile IDs to download from S3 "
                    "(e.g., 'file1,file2,file3'). Files downloaded to "
                    "output_location/project_id/ per raw_file. "
                ),
                title="Raw File IDs",
            )
        },
    ) as dag
):
    dag.doc_md = __doc__

    download_files = PythonOperator(
        task_id="download_files",
        python_callable=download_raw_files_from_s3,
        pool=Pools.S3_DOWNLOAD_POOL,
        execution_timeout=timedelta(hours=12),  # For large batches/files
    )
