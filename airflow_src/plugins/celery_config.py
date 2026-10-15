"""Celery config that also preloads heavy libraries into the worker's main process.

Task processes are forked from the worker, so modules imported here are inherited and
not re-imported on every task's DAG file parse. Only import here, never open connections
(MongoClient, boto3 sessions): these are not fork-safe.

# TODO revisit this when python 3.15 (lazy imports) is available.
"""

import mongoengine  # noqa: F401
import pymongo  # noqa: F401
from airflow.providers.celery.executors.default_celery import DEFAULT_CELERY_CONFIG

CELERY_CONFIG = DEFAULT_CELERY_CONFIG
