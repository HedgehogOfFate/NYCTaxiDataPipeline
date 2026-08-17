import argparse
import pathlib
import random

import pandas as pd

REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
RAW_DIR = REPO_ROOT / "data" / "raw"


def generate(rows: int, year: int, month: int, seed: int = 42) -> pd.DataFrame:
    rng = random.Random(seed)
    records = []
    for i in range(rows):
        day = rng.randint(1, 28)
        hour = rng.randint(0, 23)
        minute = rng.randint(0, 59)
        pickup = pd.Timestamp(year=year, month=month, day=day, hour=hour, minute=minute)
        duration_min = rng.randint(3, 45)
        dropoff = pickup + pd.Timedelta(minutes=int(duration_min))

        record = {
            "VendorID": rng.choice([1, 2]),
            "tpep_pickup_datetime": pickup,
            "tpep_dropoff_datetime": dropoff,
            "passenger_count": rng.choice([1, 1, 1, 2, 3, 4, None]),
            "trip_distance": round(rng.uniform(0.5, 15.0), 2),
            "RatecodeID": 1,
            "store_and_fwd_flag": "N",
            "PULocationID": rng.randint(1, 263),
            "DOLocationID": rng.randint(1, 263),
            "payment_type": rng.choice([1, 1, 2, 2, 3]),
            "fare_amount": round(rng.uniform(5.0, 60.0), 2),
            "extra": 0.5,
            "mta_tax": 0.5,
            "tip_amount": round(rng.uniform(0, 10.0), 2),
            "tolls_amount": 0.0,
            "improvement_surcharge": 0.3,
            "congestion_surcharge": 2.5,
        }
        record["total_amount"] = round(
            record["fare_amount"]
            + record["extra"]
            + record["mta_tax"]
            + record["tip_amount"]
            + record["tolls_amount"]
            + record["improvement_surcharge"]
            + record["congestion_surcharge"],
            2,
        )
        records.append(record)

    dirty_count = max(1, rows // 50)
    for i in range(dirty_count):
        bad = dict(records[i])
        bad["fare_amount"] = -5.0
        records.append(bad)

        bad2 = dict(records[i])
        bad2["trip_distance"] = 0.0
        records.append(bad2)

    return pd.DataFrame(records)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=500)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args()

    df = generate(args.rows, args.year, args.month)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    out_path = RAW_DIR / f"yellow_tripdata_{args.year:04d}-{args.month:02d}.parquet"

    df.to_parquet(out_path, index=False, engine="pyarrow", coerce_timestamps="us", allow_truncated_timestamps=True)
    print(f"Wrote {len(df)} synthetic rows to {out_path}")

    zone_lookup = RAW_DIR / "taxi_zone_lookup.csv"
    if not zone_lookup.exists():
        raise SystemExit(
            f"{zone_lookup} not found -- this should be committed to the repo; " "see README for where it comes from."
        )


if __name__ == "__main__":
    main()
