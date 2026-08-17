import argparse
import logging
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "spark_jobs"))

from spark_jobs.spark_utils import get_spark

from dq_checks.layer_suites import run_bronze_checks, run_gold_checks, run_silver_checks

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("run_checks")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--layer", choices=["bronze", "silver", "gold"], required=True)
    parser.add_argument("--path", required=True, help="Path to the parquet directory to validate")
    parser.add_argument("--bronze-row-count", type=int, default=None, help="Required for silver's retention check")
    parser.add_argument("--table-name", default=None, help="Required for gold (e.g. gold_borough_fares)")
    args = parser.parse_args()

    spark = get_spark(f"dq_check_{args.layer}")
    try:
        df = spark.read.parquet(args.path)

        if args.layer == "bronze":
            result = run_bronze_checks(df, raise_on_failure=False)
        elif args.layer == "silver":
            result = run_silver_checks(df, bronze_row_count=args.bronze_row_count, raise_on_failure=False)
        else:
            if not args.table_name:
                parser.error("--table-name is required for --layer gold")
            result = run_gold_checks(df, table_name=args.table_name, raise_on_failure=False)

        print(result.summary())
        if not result.success:
            log.error("DQ check FAILED for layer=%s path=%s", args.layer, args.path)
            sys.exit(1)
        log.info("DQ check PASSED for layer=%s path=%s", args.layer, args.path)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
