import argparse
import pathlib
import sys

import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
RAW_DIR = pathlib.Path(__file__).resolve().parent.parent / "data" / "raw"


def download_month(year: int, month: int, dest_dir: pathlib.Path = RAW_DIR, force: bool = False) -> pathlib.Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    fname = f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
    dest_path = dest_dir / fname
    if dest_path.exists() and not force:
        print(f"[skip] {fname} already present at {dest_path}")
        return dest_path

    url = f"{BASE_URL}/{fname}"
    print(f"[download] {url}")
    with requests.get(url, stream=True, timeout=60) as resp:
        if resp.status_code != 200:
            raise RuntimeError(f"Failed to download {url}: HTTP {resp.status_code}")
        tmp_path = dest_path.with_suffix(".parquet.tmp")
        total = int(resp.headers.get("content-length", 0))
        written = 0
        with open(tmp_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                written += len(chunk)
                if total:
                    pct = written / total * 100
                    print(f"\r  {written / 1e6:,.1f}MB / {total / 1e6:,.1f}MB ({pct:.1f}%)", end="")
        print()
        tmp_path.rename(dest_path)

    print(f"[done] wrote {dest_path} ({dest_path.stat().st_size / 1e6:,.1f}MB)")
    return dest_path


ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"


def download_zone_lookup(dest_dir: pathlib.Path = RAW_DIR, force: bool = False) -> pathlib.Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / "taxi_zone_lookup.csv"
    if dest_path.exists() and not force:
        print(f"[skip] taxi_zone_lookup.csv already present at {dest_path}")
        return dest_path
    print(f"[download] {ZONE_LOOKUP_URL}")
    resp = requests.get(ZONE_LOOKUP_URL, timeout=30)
    resp.raise_for_status()
    dest_path.write_bytes(resp.content)
    print(f"[done] wrote {dest_path}")
    return dest_path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument(
        "--month", type=int, action="append", required=True, help="Repeatable, e.g. --month 1 --month 2"
    )
    parser.add_argument("--force", action="store_true", help="Re-download even if file already exists")
    parser.add_argument("--skip-zone-lookup", action="store_true", help="Don't fetch taxi_zone_lookup.csv")
    args = parser.parse_args()

    if not args.skip_zone_lookup:
        try:
            download_zone_lookup(force=args.force)
        except Exception as exc:
            print(f"[error] zone lookup: {exc}")

    failures = []
    for month in args.month:
        try:
            download_month(args.year, month, force=args.force)
        except Exception as exc:
            print(f"[error] {args.year}-{month:02d}: {exc}")
            failures.append((args.year, month))

    if failures:
        print(f"\n{len(failures)} month(s) failed: {failures}")
        sys.exit(1)


if __name__ == "__main__":
    main()
