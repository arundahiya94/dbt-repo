#!/usr/bin/env python3
"""
DAG: fetch_gbfs_feeds
--------------------
Orchestrates the fetch → publish → archive flow for GBFS feeds
defined in src/api_to_bucket.py. Runs hourly, with retries on failure.
"""

import os
import sys
import logging
from datetime import datetime, timedelta

# ──────────────────────────────────────────────────────────────────────────────
# 1) Ensure Airflow can import your code in airflow/src
# ──────────────────────────────────────────────────────────────────────────────
DAGS_FOLDER = os.path.dirname(__file__)
AIRFLOW_HOME = os.path.abspath(os.path.join(DAGS_FOLDER, os.pardir))
SRC_PATH = os.path.join(AIRFLOW_HOME, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

# ──────────────────────────────────────────────────────────────────────────────
# 2) Standard imports
# ──────────────────────────────────────────────────────────────────────────────
from airflow import DAG
from airflow.operators.python import PythonOperator
from api_to_bucket import fetch_gbfs_feeds  # your function entry point

# ──────────────────────────────────────────────────────────────────────────────
# 3) Logger setup
# ──────────────────────────────────────────────────────────────────────────────
logger = logging.getLogger("fetch_gbfs_feeds_dag")
logger.setLevel(logging.INFO)

# ──────────────────────────────────────────────────────────────────────────────
# 4) Default task arguments
# ──────────────────────────────────────────────────────────────────────────────
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,  # retry twice on failure
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ──────────────────────────────────────────────────────────────────────────────
# 5) Wrapper to call your function, with logging
# ──────────────────────────────────────────────────────────────────────────────
def _run_fetch():
    """
    Invoke the fetch_gbfs_feeds() function from api_to_bucket.py,
    catching any exceptions to ensure Airflow records failures.
    """
    logger.info("Starting fetch_gbfs_feeds task")
    try:
        # Build a minimal Flask-like Request stub
        class DummyRequest:
            args = {}

        response = fetch_gbfs_feeds(DummyRequest())
        logger.info("fetch_gbfs_feeds completed with response: %s", response)
    except Exception as e:
        logger.error("Error in fetch_gbfs_feeds: %s", e, exc_info=True)
        # Re-raise to let Airflow mark the task as failed
        raise
    else:
        logger.info("Task succeeded, proceeding to next step")

# ──────────────────────────────────────────────────────────────────────────────
# 6) Define the DAG
# ──────────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="fetch_gbfs_feeds",
    default_args=default_args,
    description="Hourly fetch → publish → archive of Oslo GBFS feeds",
    start_date=datetime(2025, 5, 1),
    schedule="@hourly",       # Airflow 3.x requires `schedule` instead of schedule_interval
    catchup=False,            # do not backfill on first run
    max_active_runs=1,        # only one run at a time
    tags=["gbfs", "oslo", "example"],
) as dag:

    fetch_and_publish = PythonOperator(
        task_id="fetch_and_publish",
        python_callable=_run_fetch,
        provide_context=False,
    )

    # In the future you could split this into fetch → publish → archive tasks:
    # fetch = PythonOperator(...)
    # publish = PythonOperator(...)
    # archive = PythonOperator(...)
    #
    # and then set fetch >> publish >> archive

# The DAG object `dag` is automatically registered by Airflow
