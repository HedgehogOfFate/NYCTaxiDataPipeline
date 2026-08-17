import argparse
import datetime as dt
import logging
import pathlib
import shutil
import sys

from pyspark.sql import functions as F

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
from spark_utils import get_spark  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("bronze_ingest")

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"
BRONZE_DIR = REPO_ROOT / "data" / "bronze" / "yellow_tripdata"


def _warn_on_stray_files(bronze_dir: pathlib.Path) -> None:
    if not bronze_dir.exists():
        return
    stray = [p for p in bronze_dir.iterdir() if not p.name.startswith("_ingest_year=")]
    if stray:
        log.warning(
            "Found %d file(s)/dir(s) in %s that don't match the expected "
            "_ingest_year=*/_ingest_month=* partition layout: %s -- these are almost "
            "certainly leftovers from an earlier, differently-partitioned run and will "
            "be double-counted by any downstream read of this directory. Recommended "
            "fix: delete %s entirely and re-run bronze_ingest for every month you need.",
            len(stray),
            bronze_dir,
            [p.name for p in stray],
            bronze_dir,
        )


def ingest(spark, year: int, month: int, input_path: pathlib.Path, bronze_dir: pathlib.Path) -> dict:
    if not input_path.exists():
        raise FileNotFoundError(
            f"{input_path} not found. Run scripts/download_raw.py --year {year} --month {month} first."
        )

    _warn_on_stray_files(bronze_dir)

    log.info("Reading raw file: %s", input_path)
    df = spark.read.parquet(str(input_path))

    row_count = df.count()
    col_count = len(df.columns)
    file_size_mb = input_path.stat().st_size / 1e6
    log.info("Source schema: %d columns, %d rows, %.1fMB on disk", col_count, row_count, file_size_mb)
    df.printSchema()

    enriched = (
        df.withColumn("_ingest_year", F.lit(year).cast("int"))
        .withColumn("_ingest_month", F.lit(month).cast("int"))
        .withColumn("_source_file", F.lit(input_path.name))
        .withColumn("_ingested_at", F.lit(dt.datetime.now(dt.timezone.utc).isoformat()))
    )

    partition_path = bronze_dir
    target_partition_dir = bronze_dir / f"_ingest_year={year}" / f"_ingest_month={month}"
    if target_partition_dir.exists():
        log.info("Clearing existing partition directory before write: %s", target_partition_dir)
        shutil.rmtree(target_partition_dir, ignore_errors=True)

    log.info("Writing bronze partition year=%d/month=%d to %s", year, month, partition_path)

    (
        enriched.write.mode("overwrite")
        .partitionBy("_ingest_year", "_ingest_month")
        .option("partitionOverwriteMode", "dynamic")
        .parquet(str(partition_path))
    )

    metadata = {
        "year": year,
        "month": month,
        "row_count": row_count,
        "column_count": col_count,
        "source_file_size_mb": round(file_size_mb, 2),
        "source_file": input_path.name,
        "ingested_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    log.info("Bronze ingest complete: %s", metadata)
    return metadata


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--input", type=str, default=None, help="Override path to the raw parquet file")
    parser.add_argument("--bronze-dir", type=str, default=str(BRONZE_DIR))
    args = parser.parse_args()

    input_path = (
        pathlib.Path(args.input)
        if args.input
        else RAW_DIR / f"yellow_tripdata_{args.year:04d}-{args.month:02d}.parquet"
    )

    spark = get_spark("bronze_ingest")
    try:
        ingest(spark, args.year, args.month, input_path, pathlib.Path(args.bronze_dir))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
