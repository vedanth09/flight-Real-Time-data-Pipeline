#!/usr/bin/env python3
# ────────────────────────────────────────────────────────────────────────────────
#  spark_cleaning.py
#
#  Clean flight JSON data from GCS and load into BigQuery.
#
#  Author : [Your Name]
#  Created: 2025-05-04
# ────────────────────────────────────────────────────────────────────────────────

import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ────────────────────────────────────────────────────────────────────────────────
# 1. PARAMETERS
# ────────────────────────────────────────────────────────────────────────────────
GCS_JSON_PATH        = "gs://flights_live_data/live_flight_data.json"
BQ_PROJECT           = "data-management2-458610"
BQ_DATASET           = "flight_data"
BQ_TABLE             = f"{BQ_PROJECT}.{BQ_DATASET}.live_flight_cleaned"
TEMP_GCS_BUCKET      = "flights_live_data"  # Must exist and be writable by the job

# If you have a set of key columns that must be non-null, list them here:
KEY_COLUMNS          = ["flight_iata", "flight_number", "departure_scheduled"]

# ────────────────────────────────────────────────────────────────────────────────
# 2. LOGGING SET-UP
# ────────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("flight_data_cleaner")

# ────────────────────────────────────────────────────────────────────────────────
# 3. SPARK SESSION CREATION
#    (Connectors are provided via cluster --jars flags on Dataproc/EMR)
# ────────────────────────────────────────────────────────────────────────────────
def create_spark_session(app_name: str = "FlightDataCleaner") -> SparkSession:
    spark = (
        SparkSession.builder
            .appName(app_name)
            .getOrCreate()
    )
    # Allow recursive lookup in nested folders (if needed)
    spark.conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
    # Tell BigQuery connector where to stage temporary files
    spark.conf.set("temporaryGcsBucket", TEMP_GCS_BUCKET)
    return spark

# ────────────────────────────────────────────────────────────────────────────────
# 4. DATA LOADING
# ────────────────────────────────────────────────────────────────────────────────
def load_json(spark: SparkSession, path: str):
    logger.info("Reading JSON data from %s", path)
    try:
        df = (
            spark.read
                .option("multiLine", "true")
                .option("recursiveFileLookup", "true")
                .json(path)
                .withColumn("ingest_datetime", F.current_timestamp())
        )
        count = df.count()
        logger.info("Successfully read %d rows", count)
        return df
    except Exception:
        logger.exception("Failed to read JSON from %s", path)
        raise

# ────────────────────────────────────────────────────────────────────────────────
# 5. DATA CLEANING
# ────────────────────────────────────────────────────────────────────────────────
def clean_nulls(df):
    """
    Drop rows that are completely empty, then ensure key columns are non-null.
    """
    # 5.1 Drop rows where *all* columns are null
    df_clean = df.na.drop(how="all")
    rows_after_drop_all = df_clean.count()
    logger.info("Rows after drop(how='all'): %d", rows_after_drop_all)

    # 5.2 Drop rows where KEY_COLUMNS are null
    if KEY_COLUMNS:
        logger.info("Dropping rows missing any of key columns: %s", KEY_COLUMNS)
        df_clean = df_clean.na.drop(how="any", subset=KEY_COLUMNS)
        rows_after_key_drop = df_clean.count()
        logger.info("Rows after drop on key columns: %d", rows_after_key_drop)

    # 5.3 Fill default values for some specific fields (optional)
    df_clean = df_clean.fillna({
        "departure_delay": 0,
        "arrival_delay": 0,
        "aircraft_registration": "N/A",
        "aircraft_iata": "N/A",
        "aircraft_icao": "N/A",
        "live_updated": "N/A",
        "live_latitude": "N/A",
        "live_longitude": "N/A",
        "live_altitude": "N/A",
        "live_direction": "N/A",
        "live_speed_horizontal": "N/A",
        "live_speed_vertical": "N/A",
        "live_is_ground": "N/A"
    })
    
    return df_clean

# ────────────────────────────────────────────────────────────────────────────────
# 6. WRITE TO BIGQUERY
# ────────────────────────────────────────────────────────────────────────────────
def write_to_bigquery(df, table: str):
    logger.info("Writing %d rows to BigQuery table %s …", df.count(), table)
    try:
        (
            df.write.format("bigquery")
              .option("table", table)
              .option("writeMethod", "indirect")               # use GCS staging
              .option("temporaryGcsBucket", TEMP_GCS_BUCKET)   # must match spark.conf
              .option("createDisposition", "CREATE_IF_NEEDED")
              .option("schemaUpdateOption", "ALLOW_FIELD_ADDITION")
              .mode("append")
              .save()
        )
        logger.info("Write to BigQuery succeeded")
    except Exception:
        logger.exception("Failed to write to BigQuery table %s", table)
        raise

# ────────────────────────────────────────────────────────────────────────────────
# 7. MAIN EXECUTION
# ────────────────────────────────────────────────────────────────────────────────
def main():
    spark = create_spark_session()
    try:
        df_raw   = load_json(spark, GCS_JSON_PATH)
        df_clean = clean_nulls(df_raw)
        write_to_bigquery(df_clean, BQ_TABLE)
        logger.info("Flight data cleaning job completed successfully ✅")
    except Exception:
        logger.error("Flight data cleaning job failed ❌", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
