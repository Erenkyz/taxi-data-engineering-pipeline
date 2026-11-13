-- Our first data quality checks. Raw records are preserved for auditing.
SELECT
    COUNT(*) AS raw_records,
    COUNT(*) FILTER (WHERE pickup_datetime IS NULL OR dropoff_datetime IS NULL) AS missing_timestamps,
    COUNT(*) FILTER (WHERE dropoff_datetime < pickup_datetime) AS negative_duration,
    COUNT(*) FILTER (WHERE trip_distance <= 0 OR trip_distance IS NULL) AS nonpositive_distance,
    COUNT(*) FILTER (WHERE total_amount < 0 OR total_amount IS NULL) AS negative_total
FROM raw.yellow_taxi_trips;
