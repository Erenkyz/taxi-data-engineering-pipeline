"""Download a monthly official NYC TLC yellow-taxi Parquet file and load a bounded sample into PostgreSQL.

Usage: python -m src.ingest --year 2024 --month 1 --limit 20000

The same ingestion module is used locally and by the Airflow DAG.
It is idempotent for a given source filename and row-number sample.
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import psycopg
import pyarrow.parquet as pq
import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "total_amount",
    "PULocationID",
    "DOLocationID",
]
ROOT = Path(__file__).resolve().parents[1]

CREATE_SQL = """
CREATE SCHEMA IF NOT EXISTS raw;
CREATE TABLE IF NOT EXISTS raw.yellow_taxi_trips (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    source_file TEXT NOT NULL,
    source_row_number BIGINT NOT NULL,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    trip_distance DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    pickup_location_id INTEGER,
    dropoff_location_id INTEGER,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT yellow_taxi_source_row_unique UNIQUE (source_file, source_row_number)
);
"""

CREATE_TEMP_SQL = """
CREATE TEMP TABLE taxi_incoming (
    source_file TEXT NOT NULL,
    source_row_number BIGINT NOT NULL,
    pickup_datetime TIMESTAMP,
    dropoff_datetime TIMESTAMP,
    trip_distance DOUBLE PRECISION,
    total_amount DOUBLE PRECISION,
    pickup_location_id INTEGER,
    dropoff_location_id INTEGER
) ON COMMIT DROP;
"""

COPY_SQL = """
COPY taxi_incoming (
    source_file, source_row_number, pickup_datetime, dropoff_datetime,
    trip_distance, total_amount, pickup_location_id, dropoff_location_id
) FROM STDIN
"""

INSERT_SQL = """
INSERT INTO raw.yellow_taxi_trips (
    source_file, source_row_number, pickup_datetime, dropoff_datetime,
    trip_distance, total_amount, pickup_location_id, dropoff_location_id
)
SELECT source_file, source_row_number, pickup_datetime, dropoff_datetime,
       trip_distance, total_amount, pickup_location_id, dropoff_location_id
FROM taxi_incoming
ON CONFLICT (source_file, source_row_number) DO NOTHING;
"""


def download_file(year: int, month: int) -> Path:
    file_name = f"yellow_tripdata_{year}-{month:02d}.parquet"
    outdir = ROOT / "data"
    outdir.mkdir(exist_ok=True)
    destination = outdir / file_name
    if destination.is_file() and destination.stat().st_size > 0:
        print(f"Using existing file: {destination}")
        return destination

    url = f"{BASE_URL}/{file_name}"
    temp = destination.with_suffix(".parquet.part")
    print(f"Downloading official TLC data: {url}")
    try:
        with requests.get(url, stream=True, timeout=(15, 90)) as response:
            response.raise_for_status()
            with temp.open("wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        handle.write(chunk)
        temp.replace(destination)
    except Exception:
        temp.unlink(missing_ok=True)
        raise
    print(f"Downloaded {destination.stat().st_size / (1024 * 1024):.1f} MB")
    return destination


def db_connection() -> psycopg.Connection:
    return psycopg.connect(
        host=os.getenv("PGHOST", "127.0.0.1"),
        port=int(os.getenv("PGPORT", "5433")),
        dbname=os.getenv("PGDATABASE", "nyc_taxi"),
        user=os.getenv("PGUSER", "taxi_dev"),
        password=os.getenv("PGPASSWORD", "local_demo_password"),
        connect_timeout=10,
    )


def ingest(file_path: Path, limit: int) -> None:
    parquet = pq.ParquetFile(file_path)
    missing = sorted(set(COLUMNS).difference(parquet.schema_arrow.names))
    if missing:
        raise ValueError(f"Missing expected columns: {missing}")

    count = 0
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(CREATE_SQL)
            cur.execute(CREATE_TEMP_SQL)
            with cur.copy(COPY_SQL) as copy:
                for batch in parquet.iter_batches(batch_size=2_000, columns=COLUMNS):
                    values = batch.to_pydict()
                    for index in range(batch.num_rows):
                        if count >= limit:
                            break
                        copy.write_row((
                            file_path.name,
                            count,  # Stable row index inside source Parquet file
                            values["tpep_pickup_datetime"][index],
                            values["tpep_dropoff_datetime"][index],
                            values["trip_distance"][index],
                            values["total_amount"][index],
                            values["PULocationID"][index],
                            values["DOLocationID"][index],
                        ))
                        count += 1
                    if count >= limit:
                        break
            cur.execute(INSERT_SQL)
            newly_inserted = cur.rowcount
            cur.execute("SELECT COUNT(*) FROM raw.yellow_taxi_trips")
            total_count = cur.fetchone()[0]
    print(f"Read from Parquet: {count:,} source rows")
    print(f"Newly inserted:    {newly_inserted:,} rows")
    print(f"Total in database: {total_count:,} rows")
    print("Done. Repeat the same command: newly inserted should be 0 (idempotent).")


def main() -> None:
    parser = argparse.ArgumentParser(description="Load official NYC yellow-taxi records into local PostgreSQL")
    parser.add_argument("--year", type=int, default=2024)
    parser.add_argument("--month", type=int, default=1)
    parser.add_argument("--limit", type=int, default=20_000)
    args = parser.parse_args()
    if not 1 <= args.month <= 12:
        parser.error("--month must be between 1 and 12")
    if args.year < 2019:
        parser.error("Choose a year from 2019 onwards (Parquet schema).")
    if args.limit <= 0:
        parser.error("--limit must be a positive integer")
    path = download_file(args.year, args.month)
    ingest(path, args.limit)


if __name__ == "__main__":
    main()
