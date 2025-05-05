#!/usr/bin/env python
# ────────────────────────────────────────────────────────────────────────────────
#  pyspark_gbfs_raw_load.py
#
#  Ingest Oslo Bysykkel GBFS feeds (station_status & station_information) from
#  Google Cloud Storage into BigQuery “raw” tables, then delete the source files.
#
#  Author : Arun Kumar
#  Updated: 2025-05-05
# ────────────────────────────────────────────────────────────────────────────────

import logging
import sys
from urllib.parse import urlparse

from pyspark.sql import SparkSession, functions as F, types as T
from google.cloud import storage  # <-- new

# ────────────────────────────────────────────────────────────────────────────────
# 1. PARAMETERS
# ────────────────────────────────────────────────────────────────────────────────
GCS_BUCKET            = "data-management-2-arun"   # no gs:// here
STATION_STATUS_PATH   = "data/gbfs/station_status"
STATION_INFO_PATH     = "data/gbfs/station_information"

# BigQuery
BQ_PROJECT       = "data-management-2-arun"
BQ_DATASET       = "data_management_2_arun"
BQ_TABLE_STATUS  = f"{BQ_PROJECT}.{BQ_DATASET}.raw_station_status"
BQ_TABLE_INFO    = f"{BQ_PROJECT}.{BQ_DATASET}.raw_station_information"

# Temporary bucket for BigQuery connector
TEMP_GCS_BUCKET  = "gs://data-management-2-arun-temp"

# ────────────────────────────────────────────────────────────────────────────────
# 2. LOGGING SET-UP
# ────────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("gbfs_raw_loader")

# ────────────────────────────────────────────────────────────────────────────────
# 3. SPARK SESSION
# ────────────────────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder.appName("gbfs-raw-loader")
    .getOrCreate()
)
spark.conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
spark.conf.set("temporaryGcsBucket", TEMP_GCS_BUCKET)

# ────────────────────────────────────────────────────────────────────────────────
# 4. GCS DELETION HELPER
# ────────────────────────────────────────────────────────────────────────────────
storage_client = storage.Client()

def delete_blobs_in_prefix(bucket_name: str, prefix: str) -> None:
    """Deletes all blobs in the bucket that begin with the prefix."""
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    deleted = 0
    for blob in blobs:
        blob.delete()
        deleted += 1
    logger.info("Deleted %d blobs under gs://%s/%s", deleted, bucket_name, prefix)

# ────────────────────────────────────────────────────────────────────────────────
# 5. HELPER FUNCTIONS FOR INGEST + CLEANUP
# ────────────────────────────────────────────────────────────────────────────────

def strip_station_area(df):
    """Return DataFrame where data.stations[*].station_area has been dropped."""
    st_schema: T.StructType = df.select("data.stations").schema[0].dataType.elementType
    kept_fields = [F.col("s." + f.name) for f in st_schema if f.name != "station_area"]
    return df.withColumn(
        "data",
        F.struct(
            F.transform("data.stations", lambda s: F.struct(*kept_fields)).alias("stations")
        ),
    )

def load_and_write(source_prefix: str, target_table: str, drop_geo=False) -> None:
    """
    1. Load JSON files from gs://<GCS_BUCKET>/<source_prefix>
    2. Append to BigQuery table `target_table`
    3. Delete the source files under that prefix
    """
    full_path = f"gs://{GCS_BUCKET}/{source_prefix}"
    logger.info("Reading JSON files from %s …", full_path)

    df = (
        spark.read
             .option("multiLine", "true")
             .option("recursiveFileLookup", "true")
             .json(full_path)
             .withColumn("ingest_datetime", F.current_timestamp())
    )

    if drop_geo:
        logger.info("Serialising full `data` object to JSON to avoid nested arrays")
        df = df.withColumn("data_json", F.to_json("data")).drop("data")

    row_cnt = df.count()
    logger.info("Loaded %s row(s) from %s", row_cnt, full_path)

    # write to BigQuery
    df.write.format("bigquery") \
        .mode("append") \
        .option("writeMethod", "direct") \
        .save(target_table)
    logger.info("Successfully wrote %s row(s) to %s", row_cnt, target_table)

    # delete source files
    try:
        delete_blobs_in_prefix(GCS_BUCKET, source_prefix)
    except Exception:
        logger.exception("Failed to delete source files under %s", full_path)
        # decide: either continue or raise. Here we continue.

# ────────────────────────────────────────────────────────────────────────────────
# 6. MAIN
# ────────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    try:
        load_and_write(STATION_STATUS_PATH, BQ_TABLE_STATUS)
        load_and_write(STATION_INFO_PATH, BQ_TABLE_INFO, drop_geo=True)
        logger.info("GBFS raw ingestion job completed OK ✅ ")
    except Exception:
        logger.exception("GBFS raw ingestion job FAILED ❌")
        sys.exit(1)
    finally:
        spark.stop()
