import datetime as dt

import pytest
from spark_jobs.gold_aggregate import build_borough_fares, build_revenue_trends, build_tip_by_payment
from pyspark.sql import Row
from spark_jobs.silver_transform import add_derived_columns, clean_and_filter, enforce_schema


def _sample_rows():
    good = Row(
        VendorID=1,
        tpep_pickup_datetime=dt.datetime(2026, 5, 10, 8, 0, 0),
        tpep_dropoff_datetime=dt.datetime(2026, 5, 10, 8, 20, 0),
        passenger_count=2,
        trip_distance=3.5,
        RatecodeID=1,
        store_and_fwd_flag="N",
        PULocationID=100,
        DOLocationID=200,
        payment_type=1,
        fare_amount=15.0,
        extra=0.5,
        mta_tax=0.5,
        tip_amount=3.0,
        tolls_amount=0.0,
        improvement_surcharge=0.3,
        total_amount=19.3,
        congestion_surcharge=2.5,
        _ingest_year=2026,
        _ingest_month=5,
    )
    negative_fare = good.asDict()
    negative_fare["fare_amount"] = -5.0
    zero_distance = good.asDict()
    zero_distance["trip_distance"] = 0.0
    bad_timestamp = good.asDict()
    bad_timestamp["tpep_pickup_datetime"] = dt.datetime(2009, 1, 1, 0, 0, 0)
    bad_timestamp["tpep_dropoff_datetime"] = dt.datetime(2009, 1, 1, 0, 20, 0)
    duplicate = good.asDict()

    return [good.asDict(), negative_fare, zero_distance, bad_timestamp, duplicate]


@pytest.fixture
def raw_df(spark):
    return spark.createDataFrame([Row(**r) for r in _sample_rows()])


def test_enforce_schema_casts_expected_columns(raw_df):
    df = enforce_schema(raw_df)
    schema_names = set(df.columns)
    assert {"VendorID", "fare_amount", "trip_distance", "PULocationID"}.issubset(schema_names)
    assert dict(df.dtypes)["fare_amount"] == "double"
    assert dict(df.dtypes)["VendorID"] == "int"


def test_clean_and_filter_drops_bad_rows(raw_df):
    df = enforce_schema(raw_df)
    cleaned, start_rows, end_rows = clean_and_filter(df)
    assert start_rows == 5
    assert end_rows == 1
    row = cleaned.collect()[0]
    assert row["fare_amount"] == 15.0
    assert row["trip_distance"] == 3.5


def test_derived_columns_computed_correctly(raw_df):
    df = enforce_schema(raw_df)
    cleaned, _, _ = clean_and_filter(df)
    enriched = add_derived_columns(cleaned)
    row = enriched.collect()[0]
    assert row["trip_duration_minutes"] == pytest.approx(20.0)
    assert row["trip_speed_mph"] == pytest.approx(3.5 / (20.0 / 60.0))
    assert row["pickup_hour"] == 8
    assert row["tip_percentage"] == pytest.approx(20.0)


def test_null_passenger_count_imputed_to_one(spark, raw_df):
    row = _sample_rows()[0]
    row["passenger_count"] = None
    df = spark.createDataFrame([Row(**row)], schema=raw_df.schema)
    df = enforce_schema(df)
    cleaned, _, _ = clean_and_filter(df)
    assert cleaned.collect()[0]["passenger_count"] == 1


def _silver_like_rows():
    return [
        Row(
            pickup_date=dt.date(2026, 5, 10),
            pickup_borough="Manhattan",
            trip_distance=3.5,
            trip_duration_minutes=20.0,
            fare_amount=15.0,
            tip_amount=3.0,
            tip_percentage=20.0,
            total_amount=19.3,
            payment_type=1,
        ),
        Row(
            pickup_date=dt.date(2026, 5, 10),
            pickup_borough="Manhattan",
            trip_distance=1.5,
            trip_duration_minutes=10.0,
            fare_amount=9.0,
            tip_amount=0.0,
            tip_percentage=0.0,
            total_amount=10.8,
            payment_type=2,
        ),
    ]


@pytest.fixture
def silver_like_df(spark):
    return spark.createDataFrame(_silver_like_rows())


def test_build_borough_fares_aggregates_correctly(silver_like_df):
    result = build_borough_fares(silver_like_df).collect()
    assert len(result) == 1
    row = result[0]
    assert row["pickup_borough"] == "Manhattan"
    assert row["trip_count"] == 2
    assert row["avg_fare"] == pytest.approx(12.0)


def test_build_revenue_trends_sums_correctly(silver_like_df):
    result = build_revenue_trends(silver_like_df).collect()
    assert len(result) == 1
    row = result[0]
    assert row["trip_count"] == 2
    assert row["total_fare"] == pytest.approx(24.0)
    assert row["total_tips"] == pytest.approx(3.0)


def test_build_tip_by_payment_splits_by_payment_type(silver_like_df):
    result = {row["payment_type"]: row for row in build_tip_by_payment(silver_like_df).collect()}
    assert set(result.keys()) == {1, 2}
    assert result[1]["payment_type_label"] == "Credit card"
    assert result[1]["avg_tip_pct"] == pytest.approx(20.0)
    assert result[2]["payment_type_label"] == "Cash"
    assert result[2]["avg_tip_pct"] == pytest.approx(0.0)
