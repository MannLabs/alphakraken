"""DAG to upload files to S3."""

from __future__ import annotations

from datetime import timedelta

from airflow.models import Param
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from common.constants import (
    AIRFLOW_QUEUE_PREFIX,
    Pools,
)
from common.keys import (
    DagParams,
    Dags,
    Tasks,
)
from dags.impl.s3_uploader_impl import upload_raw_file_to_s3

with DAG(
    Dags.S3_UPLOADER,
    schedule=None,
    catchup=False,
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "queue": f"{AIRFLOW_QUEUE_PREFIX}s3_uploader",
    },
    description="Upload raw files to S3.",
    tags=["s3"],
    params={
        DagParams.RAW_FILE_ID: Param(type="string", minimum=3),
        # TODO: this can in principle be also re-constructed from the RawFile
        DagParams.INTERNAL_TARGET_FOLDER_PATH: Param(type="string", minimum=3),
    },
) as dag:
    dag.doc_md = __doc__

    upload_to_s3_ = PythonOperator(
        task_id=Tasks.UPLOAD_TO_S3,
        python_callable=upload_raw_file_to_s3,
        execution_timeout=timedelta(hours=6),  # Large files need time
        retries=3,
        retry_delay=timedelta(minutes=15),
        pool=Pools.S3_UPLOAD_POOL,
    )

upload_to_s3_  # noqa: B018
