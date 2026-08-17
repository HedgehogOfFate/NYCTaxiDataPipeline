import argparse
import logging
import pathlib
import sys

from pyspark.sql import functions as F

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from spark_utils import get_spark, postgres_jdbc_options  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("gold_aggregate")

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
SILVER_DIR = REPO_ROOT / "data" / "silver" / "yellow_tripdata"
GOLD_DIR = REPO_ROOT / "data" / "gold"

PAYMENT_TYPE_LABELS = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}

def build_daily_zone_trips(df):
    return df.groupBy("pickup_date", "PULocationID", "pickup_zone", "pickup_borough").agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
        F.round(F.sum("total_amount"), 2).alias("total_revenue"),
    )

def build_hourly_trips(df):
    return (
        df.groupBy("pickup_date", "pickup_hour")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        )
        .orderBy("pickup_date", "pickup_hour")
    )

def build_borough_fares(df):
    return df.groupBy("pickup_borough").agg(
        F.count("*").alias("trip_count"),
        F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
        F.round(F.avg("trip_distance"), 2).alias("avg_trip_distance"),
        F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
    )

def build_revenue_trends(df):
    return (
        df.groupBy("pickup_date")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.sum("fare_amount"), 2).alias("total_fare"),
            F.round(F.sum("tip_amount"), 2).alias("total_tips"),
            F.round(F.sum("total_amount"), 2).alias("total_revenue"),
        )
        .orderBy("pickup_date")
    )

def build_tip_by_payment(df):
    label_expr = F.create_map([F.lit(x) for pair in PAYMENT_TYPE_LABELS.items() for x in pair])
    return (
        df.withColumn("payment_type_label", label_expr[F.col("payment_type")])
        .groupBy("payment_type", "payment_type_label")
        .agg(
            F.count("*").alias("trip_count"),
            F.round(F.avg("tip_percentage"), 2).alias("avg_tip_pct"),
        )
    )


TABLES = {
    "gold_daily_zone_trips": build_daily_zone_trips,
    "gold_hourly_trips": build_hourly_trips,
    "gold_borough_fares": build_borough_fares,
    "gold_revenue_trends": build_revenue_trends,
    "gold_tip_by_payment": build_tip_by_payment,
}


def aggregate(spark, silver_dir: pathlib.Path, gold_dir: pathlib.Path, write_postgres: bool) -> dict:
    log.info("Reading silver layer: %s", silver_dir)
    df = spark.read.parquet(str(silver_dir))
    df.cache()
    row_count = df.count()
    log.info("Silver rows read: %d", row_count)

    row_counts = {}
    for table_name, builder in TABLES.items():
        result_df = builder(df)
        result_df.cache()
        n = result_df.count()
        row_counts[table_name] = n
        log.info("Built %s: %d rows", table_name, n)

        out_path = gold_dir / table_name
        result_df.write.mode("overwrite").parquet(str(out_path))
        log.info("Wrote parquet copy: %s", out_path)

        if write_postgres:
            opts = postgres_jdbc_options(table_name)
            log.info("Writing %s to Postgres via JDBC (%s)", table_name, opts["url"])
            (result_df.write.format("jdbc").options(**opts).mode("overwrite").save())

        result_df.unpersist()

    df.unpersist()
    return {"silver_rows": row_count, "gold_table_rows": row_counts}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-dir", type=str, default=str(SILVER_DIR))
    parser.add_argument("--gold-dir", type=str, default=str(GOLD_DIR))
    parser.add_argument(
        "--no-postgres",
        action="store_true",
        help="Skip the JDBC write (useful when running standalone without the docker-compose Postgres service)",
    )
    args = parser.parse_args()

    spark = get_spark("gold_aggregate")
    try:
        aggregate(
            spark, pathlib.Path(args.silver_dir), pathlib.Path(args.gold_dir), write_postgres=not args.no_postgres
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
