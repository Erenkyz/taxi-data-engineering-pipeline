SELECT
    id,
    source_file,
    source_row_number,
    pickup_datetime,
    dropoff_datetime,
    trip_distance,
    total_amount,
    pickup_location_id,
    dropoff_location_id,
    ingested_at
FROM {{ source('raw', 'yellow_taxi_trips') }}
WHERE
    EXTRACT(YEAR FROM pickup_datetime) = {{ var('year', 2026) }}
    AND EXTRACT(MONTH FROM pickup_datetime) IN (
        {{ var('months', [7, 8, 9]) | join(', ') }}
    )
    AND dropoff_datetime > pickup_datetime
    AND trip_distance > 0
    AND total_amount >= 0
