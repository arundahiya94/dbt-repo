#!/usr/bin/env python3
"""
DAG: fetch_gbfs_feeds
--------------------
Every 2 minutes: fetch → publish → archive Oslo GBFS feeds.
"""

import os
import sys
import logging
from datetime import datetime, timedelta

# 1) Put airflow/src on PYTHONPATH
DAGS_FOLDER = os.path.dirname(__file__)
AIRFLOW_HOME = os.path.abspath(os.path.join(DAGS_FOLDER, os.pardir))
SRC_PATH = os.path.join(AIRFLOW_HOME, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

# 2) Imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from api_to_bucket import fetch_gbfs_feeds  # now safe to import

# 3) Logger
logger = logging.getLogger("fetch_gbfs_feeds_dag")
logger.setLevel(logging.INFO)

# 4) Default args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
    "email_on_retry": False,
}

# 5) Task function
def _run_fetch():
    logger.info("▶️  Triggering fetch_gbfs_feeds")
    class DummyRequest:
        args = {}
    body, code = fetch_gbfs_feeds(DummyRequest())
    if code != 200:
        logger.error("fetch_gbfs_feeds returned non-OK status %s: %s", code, body)
        raise RuntimeError(f"Fetch failed: {code}")
    logger.info("fetch_gbfs_feeds succeeded: %s", body)

# 6) DAG definition
with DAG(
    dag_id="fetch_gbfs_feeds",
    default_args=default_args,
    description="Every 2 min: fetch → publish → archive Oslo GBFS feeds",
    start_date=datetime(2025, 5, 1, 0, 0),
    schedule="*/2 * * * *",  # every 2 minutes
    catchup=False,
    max_active_runs=1,
    tags=["gbfs", "oslo", "frequent"],
) as dag:

    fetch_and_publish = PythonOperator(
        task_id="fetch_and_publish",
        python_callable=_run_fetch,
    )
