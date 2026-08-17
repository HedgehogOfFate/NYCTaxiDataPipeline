# NYC-Taxi-Data-Pipeline

End-to-end batch data pipeline over NYC TLC Yellow Taxi trip data, built with
PySpark on a medallion (bronze/silver/gold) architecture. Orchestrated with
Airflow, served from Postgres, containerized with Docker Compose.

## Why this dataset

NYC TLC Yellow Taxi trip data ships as monthly parquet files (~2-3GB each,
several years of history — easily tens of GB in aggregate). It has real
volume, a natural join (trip locations -> taxi zone lookup), and realistic
messiness (nulls, negative fares, zero-distance trips, occasional corrupted
timestamps) — enough to make PySpark a genuine tool of choice rather than
something you could just as well do in pandas.

## Architecture

```
                    ┌──────────────┐
  TLC public URLs → │ download_raw │ → data/raw/*.parquet
                    └──────────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │ bronze_ingest.py│  land raw files untouched,
                  │                 │  partitioned by year=/month=
                  └─────────────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │ dq_check_bronze │  schema/lineage checks
                  └─────────────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │silver_transform │  schema enforcement, cleaning,
                  │                 │  outlier filtering, dedup, zone join,
                  └─────────────────┘  derived columns
                           │
                           ▼
                  ┌─────────────────┐
                  │ dq_check_silver │  value ranges, dedup,
                  └─────────────────┘  bronze->silver retention delta
                           │
                           ▼
                  ┌─────────────────┐
                  │ gold_aggregate  │  business aggregates
                  │                 │  → Postgres (JDBC) + parquet
                  └─────────────────┘
                           │
                           ▼
                  ┌─────────────────┐
                  │  dq_check_gold  │  per-table sanity checks
                  └─────────────────┘  (one per gold table)
```

All of the above is wired as a single Airflow DAG (`dags/taxi_pipeline_dag.py`), scheduled `@monthly`, with retries and a failure-notification stub.
CI (`.github/workflows/ci.yml`) lints, runs the pytest suite, and runs
this whole chain against a small synthetic sample file on every push.

Spark runs against a `spark-master` + `spark-worker` Docker Compose cluster
(falls back to local `local[*]` mode automatically if `SPARK_MASTER_URL` isn't
set — see `spark_jobs/spark_utils.py` — which is what makes the jobs runnable
standalone / in tests without the full compose stack up).

## Repo structure

```
de-portfolio-pipeline/
├── docker-compose.yml       # spark-master, spark-worker, postgres, airflow
├── Dockerfile.airflow       # Airflow image w/ pyspark + postgres JDBC driver
├── requirements.txt
├── pyproject.toml           # ruff + black config
├── dags/
│   └── taxi_pipeline_dag.py # full orchestrated DAG
├── spark_jobs/
│   ├── spark_utils.py       # shared SparkSession + JDBC config helpers
│   ├── bronze_ingest.py     
│   ├── silver_transform.py  
│   └── gold_aggregate.py    
├── dq_checks/                    
│   ├── expectation_suite.py      # reusable GE-styled expectation framework
│   ├── layer_suites.py           # concrete bronze/silver/gold suites
│   ├── run_checks.py             # CLI entry point (used by Airflow + humans)
│   └── count_rows.py             # tiny helper feeding silver's retention check
├── scripts/
│   ├── download_raw.py           # pulls monthly TLC parquet + zone lookup CSV
│   ├── make_sample_data.py       # synthetic sample for CI
│   └── init-multiple-dbs.sh      # provisions the `gold` Postgres database
├── tests/                    
│   ├── conftest.py                 # shared SparkSession fixture
│   ├── test_transformations.py     # silver + gold build-function unit tests
│   ├── test_schema.py              # schema/column contracts per layer boundary
│   └── test_data_quality.py        # tests the DQ framework actually catches bad data
├── data/
│   ├── raw/                 # downloaded source files
│   ├── bronze/               # untouched, partitioned by year/month
│   ├── silver/               # cleaned, joined, partitioned by pickup_date
│   └── gold/                 # aggregate tables (parquet copy of what's in Postgres)
├── .github/workflows/
│   └── ci.yml                # lint -> pytest -> pipeline smoke test
├── great_expectations/       # intentionally empty -- see dq_checks/ docstring
└── run_pipeline.py           # runs bronze->silver->gold + DQ end to end (pre-Airflow / CI)
```

## Setup

```bash
docker compose up -d
```

Brings up:
- Spark UI at `http://localhost:8080`
- Airflow UI at `http://localhost:8081` (admin/admin)
- Postgres on `localhost:5432` (`airflow` db for Airflow metadata, `gold` db for serving tables)

## Running the pipeline

```bash
# 1. Get the data (or drop a file straight into data/raw/)
python scripts/download_raw.py --year 2026 --month 5

# 2-5. Bronze → DQ → Silver → DQ → Gold → DQ, in one shot
python run_pipeline.py --year 2026 --month 5

# ...or run phases individually
python spark_jobs/bronze_ingest.py --year 2026 --month 5
python -m dq_checks.run_checks --layer bronze --path data/bronze/yellow_tripdata
python spark_jobs/silver_transform.py --year 2026 --month 5
python -m dq_checks.run_checks --layer silver --path data/silver/yellow_tripdata
python spark_jobs/gold_aggregate.py            # --no-postgres to skip the JDBC write
python -m dq_checks.run_checks --layer gold --path data/gold/gold_borough_fares --table-name gold_borough_fares
```

Each script also runs standalone (`local[*]`) without Docker Compose up, which
is what the test suite and CI rely on.

**If you hit `SparkOutOfMemoryError` running standalone:** `local[*]` mode
defaults the driver to only 1GB of heap, which isn't enough for
`gold_aggregate.py`'s five `groupBy` aggregations against a full month of data.
`spark_jobs/spark_utils.py` sets this to 4GB by default when running locally;
override with the `SPARK_DRIVER_MEMORY` env var (e.g. `SPARK_DRIVER_MEMORY=2g`
on a more memory-constrained machine, or higher for a multi-month run).

**If bronze's row count looks inflated relative to what was actually ingested:**
`bronze_ingest.py` warns (and refuses to silently continue) if it finds files in
`data/bronze/yellow_tripdata/` that don't match the expected
`_ingest_year=*/_ingest_month=*/` partition layout -- a symptom of stale files
left behind by an earlier, differently-partitioned version of the write logic
that Spark still reads on the next full-directory scan. If you see that
warning, the fix is a one-time `rm -rf data/bronze/yellow_tripdata` (or the
equivalent on Windows) followed by re-running `bronze_ingest` for every month
you need.

## Running the DAG

With `docker compose up -d` running, the `taxi_pipeline_dag` DAG appears in the
Airflow UI (`localhost:8081`, admin/admin) on a monthly schedule. To trigger a
manual run for a specific month, use the UI's "Trigger DAG w/ config" or:

```bash
docker compose exec airflow-webserver airflow dags trigger taxi_pipeline_dag \
  --exec-date 2026-05-01
```

The DAG runs `download_raw → bronze_ingest → dq_check_bronze → silver_transform
→ dq_check_silver → gold_aggregate → dq_check_gold[x5]`, with 2 retries
(5-10 min backoff) per task and a `notify_failure` task that fires if anything
in the chain fails.

**A real friction point worth knowing about going in:** getting Spark, Airflow,
and PySpark's version to actually agree turned out to be the trickiest part of
this whole project — worth walking through since it's exactly the kind of thing
interviewers ask "what went wrong" about.

- `requirements.txt` (installed locally / in CI) deliberately does **not**
  include Airflow at all. Airflow doesn't officially support running natively
  on Windows (only via WSL or Docker), so pulling its dependency tree into the
  same file a Windows dev installs locally is a real landmine, not a
  hypothetical one.
- `requirements-airflow.txt` has exactly one pin: the Airflow Spark provider,
  installed only inside `Dockerfile.airflow` (a Linux container, so the
  Windows constraint doesn't apply there).
- That provider version has to match the Airflow base image's version — an
  earlier draft of this pin (`apache-airflow-providers-apache-spark==4.11.3`)
  silently required Airflow 3.x and would have upgraded/broken the pinned
  2.9.3 base image on build. The fix was pulling Airflow's own official
  constraints file for 2.9.3 (`constraints-3.11.txt`) and using the exact
  provider/pyspark versions listed there, and passing that same file to `pip
  install --constraint` in `Dockerfile.airflow` so pip can't silently resolve
  something incompatible again later.
- `pyspark==3.5.1` (in `requirements.txt`) has to match `docker-compose.yml`'s
  `bitnami/spark:3.5` cluster image tag, since the driver (wherever pyspark
  runs) and executors (in the spark-worker container) need compatible Spark
  versions to talk to each other. If you bump one, bump the other.

## Testing & data quality

```bash
pytest tests/ -v
```

32 tests across three files:
- `test_transformations.py` — unit tests on the extracted transform/aggregate
  functions (not the Spark job scripts directly — logic is pulled into plain
  functions specifically so it's testable without spinning up bronze/silver/gold
  end to end each time)
- `test_schema.py` — asserts expected columns/types at each layer boundary
- `test_data_quality.py` — tests the DQ framework itself, including that it
  actually *fails* on intentionally broken data (a DQ suite that never fails
  is worse than no suite)

The DQ suites live in `dq_checks/` — a small hand-rolled framework in the
Great Expectations vocabulary (`expect_column_values_to_not_be_null`,
`expect_column_values_to_be_between`, etc.) rather than the actual
`great_expectations` package. That's a deliberate choice: GE's Spark execution
engine needs real context/datasource/checkpoint configuration to use well, and
pins its own pandas/pyspark version ranges that are easy to collide with — more
setup than this project's size justifies. The plan explicitly allows "a Great
Expectations suite (or hand-rolled pytest checks)"; this is that second option,
built as a small reusable library (not one-off asserts) so it's the same code
path Airflow's `dq_check_*` tasks run in production.

## CI/CD

`.github/workflows/ci.yml` runs on every push/PR to `main`:
1. **lint** — `ruff check` + `black --check`
2. **test** — the full pytest suite
3. **smoke-test** — generates a small synthetic TLC-shaped parquet file
   (`scripts/make_sample_data.py`, ~500 rows with some intentionally dirty
   ones mixed in) and runs the *entire* bronze→DQ→silver→DQ→gold→DQ chain
   against it via `run_pipeline.py`. This is a real end-to-end run, not a
   mock — it's just on synthetic data so CI doesn't need network access to
   the real TLC source or multi-GB downloads on every push.

## Design decisions

**Medallion architecture (bronze/silver/gold).** Bronze is a lossless,
append-only copy of the source — if a downstream bug ever produces wrong
numbers, bronze lets you replay from a known-good starting point instead of
re-downloading from TLC. Silver is the single "clean, typed, joined" source
of truth that any consumer (not just this pipeline) could build on. Gold is
narrow and purpose-built — each table answers one or two dashboard questions,
denormalized enough to query directly without joins.

**Explicit schema, not inference.** `silver_transform.py` casts every column
against a hand-written `StructType` rather than trusting Spark's parquet
type inference. Bronze already carries typed parquet, but an explicit schema
means a future raw file that changes a column's shape fails loudly in silver
instead of silently propagating a type drift downstream.

**Null handling.** Rows missing a pickup/dropoff timestamp, fare, or location
ID are dropped — there's no reasonable way to derive duration/revenue metrics
without them, and they're a small fraction of rows. `passenger_count` nulls
are imputed to `1` (the modal value) since it doesn't feed into fare/duration
metrics and dropping those rows would be pure data loss for no analytical
gain.

**Outlier filtering.** Negative fares, zero/negative trip distances, and
trips longer than 24 hours are dropped. Thresholds are deliberately generous
— aggressive filtering makes a dataset *look* cleaner while quietly biasing
aggregates (e.g. dropping all trips over 50 miles would erase legitimate
airport-to-suburb rides). One filter is *not* generous: pickup timestamps are
checked against the bronze ingest lineage (`_ingest_year`/`_ingest_month` —
i.e. which file this row physically came from) rather than trusted at face
value, because the May 2026 file contains a handful of rows with corrupted
2008/2009 timestamps. That's exactly the kind of real-world messiness this
dataset was chosen for.

**Deduplication.** A trip is treated as a duplicate if vendor, pickup time,
dropoff time, both location IDs, and fare all match — repartitioning /
re-ingesting the same file should never inflate trip counts.

**Broadcast join for the zone lookup.** `taxi_zone_lookup.csv` is ~265 rows
against millions of trip rows — broadcasting it avoids a shuffle join
entirely. Applied twice (pickup and dropoff) since both location IDs need
resolving to borough/zone names.

**Partitioning.** Bronze is partitioned by `_ingest_year`/`_ingest_month`
(mirrors how TLC actually ships the data — one partition per monthly file,
overwritten idempotently on re-run). Silver is partitioned by `pickup_date`
and explicitly repartitioned to 8 files before writing — the source is a
single ~4M-row month, so partitioning finer than that just creates a small-file
problem without any real parallelism benefit; `pickup_date` was chosen because
gold's aggregates group by date most often.

**Idempotency.** Bronze ingestion uses `mode("overwrite")` with
`partitionOverwriteMode=dynamic`, scoped to the `_ingest_year`/`_ingest_month`
partition being written — re-running ingestion for the same month overwrites
only that partition rather than duplicating rows or clobbering other months.

**DQ checks as a library, not inline asserts.** `dq_checks/expectation_suite.py`
is generic (works on any DataFrame), and `dq_checks/layer_suites.py` composes it
into bronze/silver/gold-specific suites. This is the same code both `run_pipeline.py`
(local runs, CI) and `dags/taxi_pipeline_dag.py` (Airflow) call — there's exactly
one place that defines "what does a clean silver row look like," not a
copy-pasted version per invocation context.

**Bronze→silver retention as a first-class check, not just eyeballing logs.**
Silver's DQ suite accepts an optional `bronze_row_count` and fails if retention
falls outside `[80%, 100%]`. That band is intentionally wide — it exists to
catch a *filter regression* (e.g. someone accidentally tightens the outlier
bounds and silently drops half the month), not to enforce a "clean-looking"
number. A silver output with 30% retention is almost certainly a bug; 96%
retention with a documented reason (like the corrupted-timestamp rows) is fine.

**BashOperator + spark-submit over SparkSubmitOperator.** SparkSubmitOperator
needs an Airflow "Spark connection" configured (host/port/extra JSON) and is,
in practice, a thin wrapper that still shells out to spark-submit. For a
single-cluster setup like this one, BashOperator makes the exact command
visible directly in the DAG file — copy-pasteable for local debugging — at the
cost of SparkSubmitOperator's connection-management abstraction, which starts
to matter more once you're juggling multiple Spark environments.

