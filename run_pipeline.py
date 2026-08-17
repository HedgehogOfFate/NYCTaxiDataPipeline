import argparse
import json
import logging
import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent / "spark_jobs"))
from spark_jobs.bronze_ingest import BRONZE_DIR, RAW_DIR, ingest  # noqa: E402
from spark_jobs.gold_aggregate import GOLD_DIR, TABLES, aggregate  # noqa: E402
from spark_jobs.silver_transform import SILVER_DIR, transform  # noqa: E402
from spark_jobs.spark_utils import get_spark  # noqa: E402

from dq_checks.layer_suites import run_bronze_checks, run_gold_checks, run_silver_checks

os.environ["HADOOP_HOME"] = "C://hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C://hadoop//bin"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("run_pipeline")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--no-postgres", action="store_true")
    parser.add_argument("--skip-dq", action="store_true", help="Skip DQ checkpoints between layers")
    args = parser.parse_args()

    input_path = RAW_DIR / f"yellow_tripdata_{args.year:04d}-{args.month:02d}.parquet"
    spark = get_spark("run_pipeline")
    results = {}
    try:
        log.info("=== Phase 2: bronze ingest ===")
        results["bronze"] = ingest(spark, args.year, args.month, input_path, BRONZE_DIR)

        if not args.skip_dq:
            log.info("=== DQ checkpoint: bronze ===")
            bronze_df = spark.read.parquet(str(BRONZE_DIR))
            run_bronze_checks(bronze_df)

        log.info("=== Phase 3: silver transform ===")
        results["silver"] = transform(spark, BRONZE_DIR, SILVER_DIR, args.year, args.month)

        if not args.skip_dq:
            log.info("=== DQ checkpoint: silver ===")
            silver_df = spark.read.parquet(str(SILVER_DIR))
            run_silver_checks(silver_df, bronze_row_count=results["bronze"]["row_count"])

        log.info("=== Phase 4: gold aggregate ===")
        results["gold"] = aggregate(spark, SILVER_DIR, GOLD_DIR, write_postgres=not args.no_postgres)

        if not args.skip_dq:
            log.info("=== DQ checkpoint: gold ===")
            for table_name in TABLES:
                table_df = spark.read.parquet(str(GOLD_DIR / table_name))
                run_gold_checks(table_df, table_name=table_name)
    finally:
        spark.stop()

    log.info("Pipeline run complete:\n%s", json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
