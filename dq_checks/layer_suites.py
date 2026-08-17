from dq_checks.expectation_suite import ExpectationSuite, SuiteResult


def bronze_suite() -> ExpectationSuite:
    suite = ExpectationSuite("bronze_yellow_tripdata")

    for col in (
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "fare_amount",
        "PULocationID",
        "DOLocationID",
        "_ingest_year",
        "_ingest_month",
        "_source_file",
    ):
        suite.expect_column_to_exist(col)

    suite.expect_table_row_count_to_be_between(min_value=1)
    suite.expect_column_values_to_not_be_null("tpep_pickup_datetime", mostly=0.99)

    return suite


def silver_suite(bronze_row_count: int = None) -> ExpectationSuite:
    suite = ExpectationSuite("silver_yellow_tripdata")

    key_columns = [
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "fare_amount",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "pickup_date",
        "pickup_borough",
        "dropoff_borough",
    ]
    for col in key_columns:
        suite.expect_column_to_exist(col)
        suite.expect_column_values_to_not_be_null(col, mostly=1.0)

    suite.expect_column_values_to_be_between("fare_amount", min_value=0, max_value=1000)
    suite.expect_column_values_to_be_between("trip_distance", min_value=0.001, max_value=200)
    suite.expect_column_values_to_be_between("trip_duration_minutes", min_value=0, max_value=24 * 60)
    suite.expect_column_values_to_be_between("passenger_count", min_value=0, max_value=9, mostly=0.999)

    suite.expect_no_exact_duplicate_rows(
        subset=[
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "PULocationID",
            "DOLocationID",
            "fare_amount",
        ],
        severity="error",
    )

    suite.expect_table_row_count_to_be_between(min_value=1)

    if bronze_row_count:

        def retention_ok(df) -> bool:
            silver_rows = df.count()
            retention = silver_rows / bronze_row_count if bronze_row_count else 0
            return 0.80 <= retention <= 1.0

        def retention_details(df) -> str:
            silver_rows = df.count()
            retention = silver_rows / bronze_row_count if bronze_row_count else 0
            return f"silver/bronze retention = {retention:.2%} ({silver_rows}/{bronze_row_count}), expected [80%, 100%]"

        suite.expect_custom(
            "expect_bronze_to_silver_retention_within_bounds",
            fn=retention_ok,
            details_fn=retention_details,
            severity="error",
        )

    return suite


def gold_suite(table_name: str) -> ExpectationSuite:

    suite = ExpectationSuite(f"gold_{table_name}")
    suite.expect_table_row_count_to_be_between(min_value=1)

    non_negative_metric_columns = {
        "gold_daily_zone_trips": ["trip_count", "avg_fare", "avg_trip_distance", "total_revenue"],
        "gold_hourly_trips": ["trip_count", "avg_fare"],
        "gold_borough_fares": ["trip_count", "avg_fare", "avg_trip_distance", "avg_duration_minutes"],
        "gold_revenue_trends": ["trip_count", "total_fare", "total_tips", "total_revenue"],
        "gold_tip_by_payment": ["trip_count", "avg_tip_pct"],
    }.get(table_name, [])

    for col in non_negative_metric_columns:
        suite.expect_column_to_exist(col)
        suite.expect_column_values_to_not_be_null(col, mostly=1.0)
        suite.expect_column_values_to_be_between(col, min_value=0, max_value=None)

    return suite


def run_bronze_checks(df, raise_on_failure: bool = True) -> SuiteResult:
    result = bronze_suite().run(df)
    if raise_on_failure:
        result.raise_if_failed()
    return result


def run_silver_checks(df, bronze_row_count: int = None, raise_on_failure: bool = True) -> SuiteResult:
    result = silver_suite(bronze_row_count).run(df)
    if raise_on_failure:
        result.raise_if_failed()
    return result


def run_gold_checks(df, table_name: str, raise_on_failure: bool = True) -> SuiteResult:
    result = gold_suite(table_name).run(df)
    if raise_on_failure:
        result.raise_if_failed()
    return result
