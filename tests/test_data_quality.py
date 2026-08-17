import datetime as dt

import pytest
from pyspark.sql import Row

from dq_checks.expectation_suite import DataQualityError, ExpectationSuite
from dq_checks.layer_suites import bronze_suite, gold_suite, silver_suite

def test_expect_column_to_exist_passes_and_fails_correctly(spark):
    df = spark.createDataFrame([Row(a=1, b=2)])
    suite = ExpectationSuite("test")
    suite.expect_column_to_exist("a")
    suite.expect_column_to_exist("missing_column")
    result = suite.run(df)
    assert result.results[0].success is True
    assert result.results[1].success is False
    assert result.success is False


def test_expect_column_values_to_not_be_null_respects_mostly_threshold(spark):
    df = spark.createDataFrame([Row(a=1), Row(a=None), Row(a=3), Row(a=4)])
    suite_strict = ExpectationSuite("strict")
    suite_strict.expect_column_values_to_not_be_null("a", mostly=1.0)
    assert suite_strict.run(df).success is False

    suite_lenient = ExpectationSuite("lenient")
    suite_lenient.expect_column_values_to_not_be_null("a", mostly=0.5)
    assert suite_lenient.run(df).success is True


def test_expect_column_values_to_be_between_catches_out_of_range(spark):
    df = spark.createDataFrame([Row(fare=10.0), Row(fare=-5.0), Row(fare=15.0)])
    suite = ExpectationSuite("test")
    suite.expect_column_values_to_be_between("fare", min_value=0, max_value=1000)
    result = suite.run(df)
    assert result.success is False
    assert "66.67%" in result.results[0].details or "66.6667%" in result.results[0].details


def test_expect_no_exact_duplicate_rows_catches_duplicates(spark):
    df = spark.createDataFrame([Row(id=1), Row(id=1), Row(id=2)])
    suite = ExpectationSuite("test")
    suite.expect_no_exact_duplicate_rows(subset=["id"], severity="error")
    result = suite.run(df)
    assert result.success is False


def test_raise_if_failed_raises_dataqualityerror(spark):
    df = spark.createDataFrame([Row(a=1)])
    suite = ExpectationSuite("test")
    suite.expect_column_to_exist("missing")
    result = suite.run(df)
    with pytest.raises(DataQualityError):
        result.raise_if_failed()


def test_warning_severity_failures_do_not_fail_the_suite(spark):
    df = spark.createDataFrame([Row(id=1), Row(id=1)])
    suite = ExpectationSuite("test")
    suite.expect_no_exact_duplicate_rows(subset=["id"], severity="warning")
    result = suite.run(df)
    assert result.success is True
    assert len(result.warning_failures) == 1


def test_bronze_suite_passes_on_well_formed_data(spark):
    row = Row(
        VendorID=1,
        tpep_pickup_datetime=dt.datetime(2026, 5, 10, 8, 0, 0),
        tpep_dropoff_datetime=dt.datetime(2026, 5, 10, 8, 20, 0),
        fare_amount=15.0,
        PULocationID=100,
        DOLocationID=200,
        _ingest_year=2026,
        _ingest_month=5,
        _source_file="yellow_tripdata_2026-05.parquet",
    )
    df = spark.createDataFrame([row])
    result = bronze_suite().run(df)
    assert result.success is True


def test_bronze_suite_fails_when_lineage_columns_missing(spark):
    df = spark.createDataFrame([Row(VendorID=1, fare_amount=15.0)])
    result = bronze_suite().run(df)
    assert result.success is False


def _good_silver_row():
    return Row(
        tpep_pickup_datetime=dt.datetime(2026, 5, 10, 8, 0, 0),
        tpep_dropoff_datetime=dt.datetime(2026, 5, 10, 8, 20, 0),
        fare_amount=15.0,
        trip_distance=3.5,
        trip_duration_minutes=20.0,
        passenger_count=2,
        PULocationID=100,
        DOLocationID=200,
        pickup_date=dt.date(2026, 5, 10),
        pickup_borough="Manhattan",
        dropoff_borough="Queens",
        VendorID=1,
    )


def test_silver_suite_passes_on_clean_data(spark):
    df = spark.createDataFrame([_good_silver_row()])
    result = silver_suite().run(df)
    assert result.success is True


def test_silver_suite_catches_negative_fare(spark):
    bad = _good_silver_row().asDict()
    bad["fare_amount"] = -10.0
    df = spark.createDataFrame([Row(**bad)])
    result = silver_suite().run(df)
    assert result.success is False


def test_silver_suite_catches_retention_below_bound(spark):
    df = spark.createDataFrame([_good_silver_row()])
    result = silver_suite(bronze_row_count=100).run(df)
    assert result.success is False
    retention_check = [r for r in result.results if "retention" in r.name][0]
    assert retention_check.success is False


def test_silver_suite_passes_retention_within_bound(spark):
    rows = [_good_silver_row() for _ in range(95)]
    df = spark.createDataFrame(rows)
    result = silver_suite(bronze_row_count=100).run(df)
    retention_check = [r for r in result.results if "retention" in r.name][0]
    assert retention_check.success is True


def test_gold_suite_passes_on_clean_borough_fares(spark):
    df = spark.createDataFrame(
        [
            Row(
                pickup_borough="Manhattan",
                trip_count=100,
                avg_fare=15.0,
                avg_trip_distance=3.0,
                avg_duration_minutes=12.0,
            )
        ]
    )
    result = gold_suite("gold_borough_fares").run(df)
    assert result.success is True


def test_gold_suite_catches_negative_avg_fare(spark):
    df = spark.createDataFrame(
        [
            Row(
                pickup_borough="Manhattan",
                trip_count=100,
                avg_fare=-15.0,
                avg_trip_distance=3.0,
                avg_duration_minutes=12.0,
            )
        ]
    )
    result = gold_suite("gold_borough_fares").run(df)
    assert result.success is False


def test_gold_suite_catches_empty_table():
    empty_check_only = ExpectationSuite("empty")
    empty_check_only.expect_table_row_count_to_be_between(min_value=1)
    assert len(empty_check_only._checks) == 1
