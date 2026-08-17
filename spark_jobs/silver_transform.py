import argparse
import logging
import pathlib
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from spark_jobs.spark_utils import get_spark  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("silver_transform")

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
BRONZE_DIR = REPO_ROOT / "data" / "bronze" / "yellow_tripdata"
SILVER_DIR = REPO_ROOT / "data" / "silver" / "yellow_tripdata"
ZONE_LOOKUP_PATH = REPO_ROOT / "data" / "raw" / "taxi_zone_lookup.csv"

TARGET_SCHEMA = StructType(
    [
        StructField("VendorID", IntegerType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("RatecodeID", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("PULocationID", IntegerType(), True),
        StructField("DOLocationID", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType(), True),
    ]
)

MAX_FARE = 1000.0
MAX_TRIP_DISTANCE = 200.0  # miles
MAX_TRIP_HOURS = 24


def enforce_schema(df):
    select_exprs = []
    for field in TARGET_SCHEMA.fields:
        if field.name in df.columns:
            select_exprs.append(F.col(field.name).cast(field.dataType).alias(field.name))
        else:
            select_exprs.append(F.lit(None).cast(field.dataType).alias(field.name))
    lineage_cols = [c for c in ("_ingest_year", "_ingest_month", "_source_file") if c in df.columns]
    return df.select(*select_exprs, *lineage_cols)


def clean_and_filter(df):
    start_rows = df.count()

    if "_ingest_year" in df.columns and "_ingest_month" in df.columns:
        expected_start = F.make_date(F.col("_ingest_year"), F.col("_ingest_month"), F.lit(1)) - F.expr("INTERVAL 1 DAY")
        expected_end = F.add_months(F.make_date(F.col("_ingest_year"), F.col("_ingest_month"), F.lit(1)), 1) + F.expr(
            "INTERVAL 1 DAY"
        )
        df = df.filter(
            (F.to_date(F.col("tpep_pickup_datetime")) >= expected_start)
            & (F.to_date(F.col("tpep_pickup_datetime")) < expected_end)
        )

    df = df.withColumn(
        "passenger_count",
        F.when(F.col("passenger_count").isNull(), F.lit(1)).otherwise(F.col("passenger_count")),
    )
    df = df.filter(
        F.col("tpep_pickup_datetime").isNotNull()
        & F.col("tpep_dropoff_datetime").isNotNull()
        & F.col("fare_amount").isNotNull()
        & F.col("PULocationID").isNotNull()
        & F.col("DOLocationID").isNotNull()
    )

    df = df.filter(
        (F.col("fare_amount") >= 0)
        & (F.col("fare_amount") <= MAX_FARE)
        & (F.col("trip_distance") > 0)
        & (F.col("trip_distance") <= MAX_TRIP_DISTANCE)
        & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        & (
            (F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long"))
            <= MAX_TRIP_HOURS * 3600
        )
    )

    dedup_keys = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "fare_amount",
    ]
    df = df.dropDuplicates(dedup_keys)

    end_rows = df.count()
    log.info(
        "Cleaning: %d -> %d rows (%.2f%% retained)",
        start_rows,
        end_rows,
        100.0 * end_rows / start_rows if start_rows else 0.0,
    )
    return df, start_rows, end_rows


def add_derived_columns(df):
    duration_seconds = F.col("tpep_dropoff_datetime").cast("long") - F.col("tpep_pickup_datetime").cast("long")
    df = df.withColumn("trip_duration_minutes", (duration_seconds / 60.0))
    df = df.withColumn(
        "trip_speed_mph",
        F.when(duration_seconds > 0, F.col("trip_distance") / (duration_seconds / 3600.0)).otherwise(F.lit(None)),
    )
    df = df.withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    df = df.withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    df = df.withColumn("day_of_week", F.date_format("tpep_pickup_datetime", "EEEE"))
    df = df.withColumn("is_weekend", F.dayofweek("tpep_pickup_datetime").isin([1, 7]))
    df = df.withColumn(
        "tip_percentage",
        F.when(F.col("fare_amount") > 0, (F.col("tip_amount") / F.col("fare_amount")) * 100.0).otherwise(F.lit(0.0)),
    )
    return df


def join_zone_lookup(spark, df):
    zones = spark.read.option("header", True).option("inferSchema", True).csv(str(ZONE_LOOKUP_PATH))
    zones_pu = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
    )
    zones_do = zones.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("Zone").alias("dropoff_zone"),
    )
    df = df.join(F.broadcast(zones_pu), on="PULocationID", how="left")
    df = df.join(F.broadcast(zones_do), on="DOLocationID", how="left")
    return df


def transform(spark, bronze_dir: pathlib.Path, silver_dir: pathlib.Path, year: int = None, month: int = None) -> dict:
    log.info("Reading bronze layer: %s", bronze_dir)
    df = spark.read.parquet(str(bronze_dir))
    if year is not None:
        df = df.filter(F.col("_ingest_year") == year)
    if month is not None:
        df = df.filter(F.col("_ingest_month") == month)

    df = enforce_schema(df)
    df, start_rows, end_rows = clean_and_filter(df)
    df = add_derived_columns(df)
    df = join_zone_lookup(spark, df)

    df = df.repartition(8, "pickup_date")

    log.info("Writing silver layer to %s, partitioned by pickup_date", silver_dir)
    df.write.mode("overwrite").partitionBy("pickup_date").parquet(str(silver_dir))

    metadata = {
        "bronze_rows": start_rows,
        "silver_rows": end_rows,
        "rows_dropped": start_rows - end_rows,
        "retention_pct": round(100.0 * end_rows / start_rows, 2) if start_rows else 0.0,
    }
    log.info("Silver transform complete: %s", metadata)
    return metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=None)
    parser.add_argument("--month", type=int, default=None)
    parser.add_argument("--bronze-dir", type=str, default=str(BRONZE_DIR))
    parser.add_argument("--silver-dir", type=str, default=str(SILVER_DIR))
    args = parser.parse_args()

    spark = get_spark("silver_transform")
    try:
        transform(spark, pathlib.Path(args.bronze_dir), pathlib.Path(args.silver_dir), args.year, args.month)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
