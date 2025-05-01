#!/usr/bin/env python
"""gbfs_etl.py  –  End-to-end ETL for Oslo Bysykkel GBFS snapshots.

Run with (all flags optional because sane defaults are baked in):

    spark-submit gbfs_etl.py \
       --gcs-bucket gs://data-management-2-arun/data/gbfs \
       --bq-dataset data_management_2_arun \
       --bq-table   gbfs_feed_cleaned \
       --watermark-min 2
"""
# ---------------------------------------------------------------------------
# 0  Imports & utilities                                                    |
# ---------------------------------------------------------------------------
import argparse
import logging
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import IntegerType

# ---------------------------------------------------------------------------
# 1  CLI argument parsing                                                   |
# ---------------------------------------------------------------------------


def parse_args(argv):
    """Parse command-line flags, returning defaults when not provided."""
    parser = argparse.ArgumentParser("GBFS ETL to BigQuery")

    parser.add_argument("--gcs-bucket",
                        help="Base GCS path containing station_status/ & station_information/ folders")

    parser.add_argument("--bq-dataset",
                        help="Destination BigQuery dataset")

    parser.add_argument("--bq-table",
                        help="Destination BigQuery table (inside dataset)")

    parser.add_argument("--watermark-min", type=int, default=2,
                        help="Ignore station_status rows that arrive >N minutes late (default 2)")

    args = parser.parse_args(argv)

    # ------- Fallback to hard-coded defaults so the flags stay optional -------
    if not args.gcs_bucket:
        args.gcs_bucket = "gs://data-management-2-arun/data/gbfs"
    if not args.bq_dataset:
        args.bq_dataset = "data_management_2_arun"
    if not args.bq_table:
        args.bq_table = "gbfs_feed_cleaned"

    return args


# ---------------------------------------------------------------------------
# 2  Spark session factory                                                  |
# ---------------------------------------------------------------------------


def build_spark(app_name: str) -> SparkSession:
    """Return a configured SparkSession (BigQuery connector already on Dataproc)."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

# ---------------------------------------------------------------------------
# 3  Main ETL                                                               |
# ---------------------------------------------------------------------------


def main(argv=None):
    # ---------- 3.0  Parse flags & bootstrap logging ----------
    args = parse_args(argv or sys.argv[1:])
    logging.basicConfig(
        format="%(asctime)s %(levelname)s %(message)s",
        level=logging.INFO,
    )
    log = logging.getLogger(__name__)
    spark = build_spark("GBFS_ETL")
    spark.sparkContext.setLogLevel("WARN")

    # Show effective parameters right at the top of the driver log
    log.info(
        "Parameters in effect →  gcs_bucket=%s | dataset=%s | table=%s | watermark=%d min",
        args.gcs_bucket, args.bq_dataset, args.bq_table, args.watermark_min,
    )

    status_path = f"{args.gcs_bucket}/station_status"
    info_path   = f"{args.gcs_bucket}/station_information"
    target_bq   = f"{args.bq_dataset}.{args.bq_table}"

    # -------------------------------------------------------------------
    # 3.1  Load raw JSON feeds                                           |
    # -------------------------------------------------------------------
    log.info("Loading raw snapshots from GCS…")
    raw_status = (
        spark.read.option("recursiveFileLookup", "true").json(status_path)
        .withColumn("feed_time", F.to_timestamp("last_updated"))
    )
    raw_info = spark.read.option("recursiveFileLookup", "true").json(info_path)

    # -------------------------------------------------------------------
    # 3.2  Transform station_status                                      |
    # -------------------------------------------------------------------
    log.info("Transforming station_status (%d snapshots)…", raw_status.count())

    # helper UDF-free expression: sum ebike counts if present
    def ebike_count(col):
        return (
            F.expr(
                """
                aggregate(
                    filter({c}, x -> x.vehicle_type_id = 'YOS:VehicleType:ebike'),
                    0L,                               -- BIGINT seed fixes INT/BIGINT mismatch
                    (acc, x) -> acc + x.count
                )
                """.format(c=col)
            )
            .cast(IntegerType())                     # final value back to INT
        )

    tidy_status = (
        raw_status
        .selectExpr("feed_time", "explode(data.stations) AS st")
        .select(
            "feed_time",
            F.to_timestamp("st.last_reported").alias("report_time"),
            "st.station_id",
            "st.num_bikes_available",
            ebike_count("st.vehicle_types_available").alias("num_ebikes_available"),
            "st.num_docks_available",
            "st.is_installed",
            "st.is_renting",
            "st.is_returning",
        )
        .dropDuplicates(["station_id", "report_time"])
        .filter(
            (F.col("feed_time") - F.col("report_time")
             <= F.expr(f"INTERVAL {args.watermark_min} MINUTES"))
        )
        .withColumn("report_date", F.to_date("report_time"))
    )
    log.info("tidy_status rows → %d", tidy_status.count())

    # -------------------------------------------------------------------
    # 3.3  Transform station_information                                  |
    # -------------------------------------------------------------------
    enriched = (
        raw_info
        .selectExpr("explode(data.stations) AS st")
        .select("st.*")
        # ⬇️  keep station_area as JSON text (optional) -----------------
        .withColumn("station_area_json", F.to_json("station_area"))
        .drop("station_area")                       # <-- BigQuery-safe
    )

    # -------------------------------------------------------------------
    # 3.4  Write to BigQuery                                             |
    # -------------------------------------------------------------------
    log.info("Writing to BigQuery table %s…", target_bq)
    (
        enriched.write.format("bigquery")
        .option("writeMethod", "direct")
        .option("partitionField", "report_date")
        .option("clusteredFields", "station_id")
        .mode("append")
        .save(target_bq)
    )

    log.info("✅ ETL finished successfully (%d rows appended).", enriched.count())
    spark.stop()


# ---------------------------------------------------------------------------
# 4  Entrypoint                                                             |
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()
