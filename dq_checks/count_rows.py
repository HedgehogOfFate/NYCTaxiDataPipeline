import argparse
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent / "spark_jobs"))

from spark_jobs.spark_utils import get_spark


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", required=True)
    args = parser.parse_args()

    spark = get_spark("count_rows")
    try:
        count = spark.read.parquet(args.path).count()
    finally:
        spark.stop()

    print(count)


if __name__ == "__main__":
    main()
