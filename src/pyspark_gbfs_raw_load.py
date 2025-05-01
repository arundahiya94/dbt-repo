#!/usr/bin/env python
# ────────────────────────────────────────────────────────────────────────────────
#  pyspark_gbfs_raw_load.py
#
#  Ingest Oslo Bysykkel GBFS feeds (station_status & station_information) from
#  Google Cloud Storage into BigQuery “raw” tables.
#
#  Author : Arun Kumar
#  Created: 2025-05-01
# ────────────────────────────────────────────────────────────────────────────────

import logging
import sys
from pyspark.sql import SparkSession, functions as F, types as T

# ────────────────────────────────────────────────────────────────────────────────
#  1. PARAMETERS
# ────────────────────────────────────────────────────────────────────────────────
GCS_BUCKET            = "gs://data-management-2-arun/data/gbfs"
STATION_STATUS_PATH   = f"{GCS_BUCKET}/station_status"
STATION_INFO_PATH     = f"{GCS_BUCKET}/station_information"

# BigQuery
BQ_PROJECT   = "data-management-2-arun"
BQ_DATASET   = "data_management_2_arun"
BQ_TABLE_STATUS = f"{BQ_PROJECT}.{BQ_DATASET}.raw_station_status"
BQ_TABLE_INFO   = f"{BQ_PROJECT}.{BQ_DATASET}.raw_station_information"

# Temporary bucket for BigQuery connector
TEMP_GCS_BUCKET = "gs://data-management-2-arun-temp"

# ────────────────────────────────────────────────────────────────────────────────
#  2. LOGGING SET-UP
# ────────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("gbfs_raw_loader")

# ────────────────────────────────────────────────────────────────────────────────
#  3. SPARK SESSION
#     -- The BigQuery connector JAR must be on the classpath. On Dataproc you
#        can pass:  --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar
# ────────────────────────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder.appName("gbfs-raw-loader")
    .getOrCreate()
)
# Tell Spark to read files in nested folders
spark.conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")

spark.conf.set("temporaryGcsBucket", TEMP_GCS_BUCKET)

# ────────────────────────────────────────────────────────────────────────────────
# 4. HELPER FUNCTION
# ────────────────────────────────────────────────────────────────────────────────

def strip_station_area(df):
    """Return DataFrame where data.stations[*].station_area has been dropped."""
    # Schema of one station element
    st_schema: T.StructType = (
        df.select("data.stations").schema[0].dataType.elementType
    )

    kept_fields = [
        F.col("s." + f.name) for f in st_schema if f.name != "station_area"
    ]

    return df.withColumn(
        "data",
        F.struct(
            F.transform("data.stations", lambda s: F.struct(*kept_fields)).alias("stations")
        ),
    )

def load_and_write(source_path: str, target_table: str, drop_geo=False) -> None:
    """
    Load all JSON files under `source_path`, drop the `station_area` column
    if it exists, and append to BigQuery table `target_table`.
    """
    logger.info("Reading JSON files from %s …", source_path)

    df = (
        spark.read
             .option("multiLine", "true")
             .option("recursiveFileLookup", "true")
             .json(source_path)
             .withColumn("ingest_datetime", F.current_timestamp())
    )

    if drop_geo:
        logger.info("Serialising full `data` object to JSON to avoid nested arrays")
        df = (df.withColumn("data_json", F.to_json("data"))
                .drop("data"))

    row_cnt = df.count()
    logger.info("Loaded %s row(s) from %s", row_cnt, source_path)

    (df.write.format("bigquery")
             .mode("append")
             .option("writeMethod", "direct")
             .save(target_table))

    logger.info("Successfully wrote %s row(s) to %s", row_cnt, target_table)


# ────────────────────────────────────────────────────────────────────────────────
# 5. MAIN
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