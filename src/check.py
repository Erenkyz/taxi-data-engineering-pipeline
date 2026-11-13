"""Run first-round data inspection and quality checks."""

from .ingest import db_connection

QUERY = """
SELECT
    COUNT(*) AS total_rows,
    COUNT(*) FILTER (WHERE pickup_datetime IS NULL OR dropoff_datetime IS NULL) AS missing_timestamps,
    COUNT(*) FILTER (WHERE dropoff_datetime < pickup_datetime) AS reversed_trips,
    COUNT(*) FILTER (WHERE trip_distance <= 0 OR trip_distance IS NULL) AS nonpositive_or_missing_distance,
    COUNT(*) FILTER (WHERE total_amount < 0 OR total_amount IS NULL) AS negative_or_missing_total,
    MIN(pickup_datetime) AS first_pickup,
    MAX(pickup_datetime) AS last_pickup
FROM raw.yellow_taxi_trips;
"""


def main() -> None:
    with db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(QUERY)
            fields = [column.name for column in cur.description]
            values = cur.fetchone()
            for name, value in zip(fields, values):
                print(f"{name:36} {value}")
    print("Note: this is raw data. Bad-looking records are counted, not silently deleted.")


if __name__ == "__main__":
    main()
