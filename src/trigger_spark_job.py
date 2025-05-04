# File: main.py

import os
import logging
from google.cloud import dataproc_v1
import functions_framework
from flask import Request, jsonify

# Configure root logger. In Cloud Functions this will be picked up by Cloud Logging.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === Configuration ===
# These values are specific to your environment.
PROJECT_ID    = os.getenv("GCP_PROJECT", "data-management-2-arun")
REGION        = os.getenv("DATAPROC_REGION", "europe-west3")
CLUSTER_NAME  = os.getenv("DATAPROC_CLUSTER", "gbfs-spark-cluster")
PYSPARK_URI   = os.getenv(
    "PYSPARK_SCRIPT_URI",
    "gs://data-management-2-arun/gbfs/pyspark_gbfs_raw_load.py"
)

@functions_framework.http
def trigger_spark_job(request: Request):
    """
    HTTP Cloud Function to submit a PySpark job to Dataproc.
    Expects an HTTP GET or POST.
    Logs the submission result or any errors.
    """

    logger.info("Received request to trigger Dataproc PySpark job")

    # Initialize the Dataproc JobController client for the specified region
    try:
        job_client = dataproc_v1.JobControllerClient(
            client_options={
                "api_endpoint": f"{REGION}-dataproc.googleapis.com:443"
            }
        )
        logger.debug(
            "Initialized Dataproc JobControllerClient for project=%s, region=%s",
            PROJECT_ID, REGION
        )
    except Exception as e:
        logger.exception("Failed to initialize Dataproc client")
        return jsonify(error="Dataproc client init failed", details=str(e)), 500

    # Define the job payload
    job = {
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            # The GCS URI of your PySpark driver script
            "main_python_file_uri": PYSPARK_URI,
            # Additional configuration examples:
            # "args": ["--date", "2025-05-04"],
            # "jar_file_uris": ["gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"],
            # "properties": {"spark.executor.memory": "4g"}
        }
    }

    logger.info(
        "Submitting PySpark job: cluster=%s, script=%s",
        CLUSTER_NAME, PYSPARK_URI
    )

    # Submit the job
    try:
        response = job_client.submit_job(
            project_id=PROJECT_ID,
            region=REGION,
            job=job
        )
        job_id = response.reference.job_id
        state = response.status.state.name

        logger.info(
            "Job submitted: job_id=%s, initial_state=%s",
            job_id, state
        )
        return jsonify(
            submitted=True,
            job_id=job_id,
            initial_state=state
        ), 200

    except Exception as e:
        logger.exception("Error submitting job to Dataproc")
        return jsonify(error="Job submission failed", details=str(e)), 500
