import datetime as dt

import pytest
from spark_jobs.gold_aggregate import TABLES
from pyspark.sql import Row
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType
from spark_jobs.silver_transform import TARGET_SCHEMA, add_derived_columns, enforce_schema, join_zone_lookup

def test_target_schema_has_expected_business_columns():
    names = {f.name for f in TARGET_SCHEMA.fields}
    expected = {
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "payment_type",
        "fare_amount",
        "total_amount",
    }
    assert expected.issubset(names)


def test_target_schema_types_are_as_documented():
    types_by_name = {f.name: f.dataType for f in TARGET_SCHEMA.fields}
    assert isinstance(types_by_name["VendorID"], IntegerType)
    assert isinstance(types_by_name["tpep_pickup_datetime"], TimestampType)
    assert isinstance(types_by_name["fare_amount"], DoubleType)
    assert isinstance(types_by_name["store_and_fwd_flag"], StringType)


def test_enforce_schema_output_matches_target_types(spark):
    row = Row(
        VendorID="1",
        tpep_pickup_datetime=dt.datetime(2026, 5, 10, 8, 0, 0),
        tpep_dropoff_datetime=dt.datetime(2026, 5, 10, 8, 20, 0),
        fare_amount="15.0",
        _ingest_year=2026,
        _ingest_month=5,
    )
    df = spark.createDataFrame([row])
    result = enforce_schema(df)
    dtypes = dict(result.dtypes)
    assert dtypes["VendorID"] == "int"
    assert dtypes["fare_amount"] == "double"
    assert "_ingest_year" in result.columns
    assert "_ingest_month" in result.columns
    assert "trip_distance" in result.columns


def test_add_derived_columns_adds_expected_columns(spark):
    row = Row(
        tpep_pickup_datetime=dt.datetime(2026, 5, 10, 8, 0, 0),
        tpep_dropoff_datetime=dt.datetime(2026, 5, 10, 8, 20, 0),
        trip_distance=3.5,
        fare_amount=15.0,
        tip_amount=3.0,
    )
    df = spark.createDataFrame([row])
    result = add_derived_columns(df)
    for col in (
        "trip_duration_minutes",
        "trip_speed_mph",
        "pickup_date",
        "pickup_hour",
        "day_of_week",
        "is_weekend",
        "tip_percentage",
    ):
        assert col in result.columns


def test_join_zone_lookup_adds_borough_and_zone_columns(spark):
    row = Row(PULocationID=100, DOLocationID=200)
    df = spark.createDataFrame([row])
    result = join_zone_lookup(spark, df)
    for col in ("pickup_borough", "pickup_zone", "dropoff_borough", "dropoff_zone"):
        assert col in result.columns

GOLD_EXPECTED_COLUMNS = {
    "gold_daily_zone_trips": {
        "pickup_date",
        "PULocationID",
        "pickup_zone",
        "pickup_borough",
        "trip_count",
        "avg_fare",
        "avg_trip_distance",
        "total_revenue",
    },
    "gold_hourly_trips": {"pickup_date", "pickup_hour", "trip_count", "avg_fare"},
    "gold_borough_fares": {"pickup_borough", "trip_count", "avg_fare", "avg_trip_distance", "avg_duration_minutes"},
    "gold_revenue_trends": {"pickup_date", "trip_count", "total_fare", "total_tips", "total_revenue"},
    "gold_tip_by_payment": {"payment_type", "payment_type_label", "trip_count", "avg_tip_pct"},
}


@pytest.mark.parametrize("table_name", list(TABLES.keys()))
def test_gold_table_registered_with_expected_columns(table_name):
    assert table_name in GOLD_EXPECTED_COLUMNS
    assert table_name in TABLES
